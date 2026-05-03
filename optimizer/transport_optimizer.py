import psycopg2
import pandas as pd
import numpy as np
from pulp import (
    LpProblem, LpVariable, LpMinimize,
    lpSum, LpStatus, value
)

# ── Config ───────────────────────────────────────────────
DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "energy_db",
    "user":     "energy_user",
    "password": "energy_pass"
}

# Zone coordinates (simulated grid positions)
ZONE_COORDS = {
    "Zone-A": (2.0, 2.0),
    "Zone-B": (4.0, 6.0),
    "Zone-C": (7.0, 3.0),
    "Zone-D": (8.0, 7.0),
    "Zone-E": (5.0, 9.0),
}

# ── DB Helpers ───────────────────────────────────────────
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_sources(conn) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT source_name, source_type, capacity_kwh, location_x, location_y "
        "FROM public.energy_sources",
        conn
    )

def fetch_allocations(conn) -> pd.DataFrame:
    """Fetch the latest LP allocation results (Stage 1 output)"""
    return pd.read_sql("""
        SELECT DISTINCT ON (zone)
            zone, allocated_kwh, priority_score
        FROM public.energy_allocations
        ORDER BY zone, run_time DESC
    """, conn)

def save_flows(conn, flows: list):
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO public.transport_flows
            (source_name, zone, flow_kwh, transmission_loss_pct, effective_kwh, cost)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, flows)
    conn.commit()
    cursor.close()

# ── Transmission Loss ────────────────────────────────────
def transmission_loss(sx, sy, zx, zy) -> float:
    """
    Loss increases with distance.
    Formula: loss_pct = 0.01 * euclidean_distance
    Represents ~1% loss per unit of grid distance.
    """
    distance = np.sqrt((sx - zx)**2 + (sy - zy)**2)
    return round(0.01 * distance, 4)

# ── Build Cost Matrix ────────────────────────────────────
def build_cost_matrix(sources: pd.DataFrame) -> dict:
    """
    cost[source][zone] = transmission loss factor
    """
    cost = {}
    for _, src in sources.iterrows():
        cost[src["source_name"]] = {}
        for zone, (zx, zy) in ZONE_COORDS.items():
            cost[src["source_name"]][zone] = transmission_loss(
                src["location_x"], src["location_y"], zx, zy
            )
    return cost

# ── Transportation Optimizer ─────────────────────────────
def run_transport_optimizer(
    sources: pd.DataFrame,
    allocations: pd.DataFrame
) -> list:

    source_names  = sources["source_name"].tolist()
    zones         = allocations["zone"].tolist()
    supply        = dict(zip(sources["source_name"], sources["capacity_kwh"]))
    demand        = dict(zip(allocations["zone"], allocations["allocated_kwh"]))
    cost          = build_cost_matrix(sources)

    # ── Decision Variables ───────────────────────────────
    # flow[s][z] = kWh sent from source s to zone z
    flow = {
        s: {
            z: LpVariable(f"flow_{s.replace(' ', '_')}_{z.replace('-', '_')}",
                         lowBound=0)
            for z in zones
        }
        for s in source_names
    }

    # ── Problem ──────────────────────────────────────────
    prob = LpProblem("EnergyTransportation", LpMinimize)

    # Objective: minimize total transmission loss
    prob += lpSum(
        cost[s][z] * flow[s][z]
        for s in source_names
        for z in zones
    )

    # Constraint 1: supply from each source <= its capacity
    for s in source_names:
        prob += lpSum(flow[s][z] for z in zones) <= supply[s]

    # Constraint 2: each zone receives exactly its Stage 1 allocation
    for z in zones:
        prob += lpSum(flow[s][z] for s in source_names) == demand[z]

    # ── Solve ────────────────────────────────────────────
    prob.solve()
    status = LpStatus[prob.status]

    print(f"\n{'='*65}")
    print(f"  Transportation Optimizer Status: {status}")
    print(f"{'='*65}")
    print(f"{'Source':<22} {'Zone':<10} {'Flow':>8} {'Loss%':>8} {'Effective':>10}")
    print(f"{'-'*65}")

    results = []
    for s in source_names:
        for z in zones:
            f = round(value(flow[s][z]), 2)
            if f > 0.01:  # only show meaningful flows
                loss_pct  = cost[s][z]
                effective = round(f * (1 - loss_pct), 2)
                cost_val  = round(f * loss_pct, 4)
                print(f"{s:<22} {z:<10} {f:>8.2f} {loss_pct*100:>7.2f}% {effective:>10.2f}")
                results.append((s, z, float(f), float(loss_pct), float(effective), float(cost_val)))

    total_flow      = sum(r[2] for r in results)
    total_effective = sum(r[4] for r in results)
    total_loss      = round(total_flow - total_effective, 2)

    print(f"{'-'*65}")
    print(f"{'TOTAL':<22} {'':10} {total_flow:>8.2f} {'':8} {total_effective:>10.2f}")
    print(f"  Total Transmission Loss: {total_loss} kWh")
    print(f"{'='*65}\n")

    return results

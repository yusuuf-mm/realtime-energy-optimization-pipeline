import psycopg2
import pandas as pd
from pulp import (
    LpProblem, LpVariable, LpMaximize,
    lpSum, LpStatus, value
)
from datetime import datetime

# ── Config ───────────────────────────────────────────────
DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "energy_db",
    "user":     "energy_user",
    "password": "energy_pass"
}

GRID_CAPACITY_KWH = 1000.0  # Total energy budget to allocate
MIN_ALLOCATION_PCT = 0.05   # Every zone gets at least 5% of its demand

# ── DB Helpers ───────────────────────────────────────────
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_zone_demand(conn) -> pd.DataFrame:
    query = """
        SELECT
            zone,
            energy_consumed_kwh  AS demand_kwh,
            priority_score
        FROM analytics.mart_demand_priority
        ORDER BY priority_score DESC
    """
    return pd.read_sql(query, conn)

def save_allocations(conn, results: list):
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO public.energy_allocations
            (zone, demand_kwh, priority_score, allocated_kwh, allocation_pct, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, results)
    conn.commit()
    cursor.close()

# ── Optimizer ────────────────────────────────────────────
def run_optimizer(df: pd.DataFrame) -> list:
    zones          = df["zone"].tolist()
    demand         = dict(zip(df["zone"], df["demand_kwh"]))
    priority       = dict(zip(df["zone"], df["priority_score"]))
    total_demand   = sum(demand.values())

    # ── Decision Variables ───────────────────────────────
    # x[zone] = kWh allocated to that zone
    x = {
        zone: LpVariable(
            name=f"alloc_{zone.replace('-', '_')}",
            lowBound=demand[zone] * MIN_ALLOCATION_PCT,  # minimum floor
            upBound=demand[zone]                          # can't exceed demand
        )
        for zone in zones
    }

    # ── Problem Definition ───────────────────────────────
    prob = LpProblem("EnergyAllocation", LpMaximize)

    # Objective: maximize priority-weighted allocation
    prob += lpSum(priority[z] * x[z] for z in zones)

    # Constraint 1: total allocation <= grid capacity
    prob += lpSum(x[z] for z in zones) <= GRID_CAPACITY_KWH

    # Constraint 2: high priority zones get proportionally more
    for z in zones:
        if priority[z] == 3:  # high
            prob += x[z] >= demand[z] * 0.6
        elif priority[z] == 2:  # medium
            prob += x[z] >= demand[z] * 0.3
        else:  # low
            prob += x[z] >= demand[z] * MIN_ALLOCATION_PCT

    # ── Solve ────────────────────────────────────────────
    prob.solve()
    status = LpStatus[prob.status]

    print(f"\n{'='*50}")
    print(f"  Optimizer Status : {status}")
    print(f"  Grid Capacity    : {GRID_CAPACITY_KWH} kWh")
    print(f"  Total Demand     : {round(total_demand, 2)} kWh")
    print(f"{'='*50}")
    print(f"{'Zone':<10} {'Demand':>10} {'Allocated':>12} {'Pct':>8} {'Priority':>10}")
    print(f"{'-'*50}")

    results = []
    for z in zones:
        allocated = round(value(x[z]), 2)
        pct       = round((allocated / demand[z]) * 100, 1)
        print(f"{z:<10} {demand[z]:>10.2f} {allocated:>12.2f} {pct:>7.1f}% {priority[z]:>10}")
        results.append((z, demand[z], priority[z], allocated, pct, status))

    total_allocated = sum(value(x[z]) for z in zones)
    print(f"{'-'*50}")
    print(f"{'TOTAL':<10} {total_demand:>10.2f} {total_allocated:>12.2f}")
    print(f"{'='*50}\n")

    return results

# ── Main ─────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Running optimizer at {datetime.utcnow().isoformat()}")

    conn = get_connection()
    df   = fetch_zone_demand(conn)

    print(f"Loaded {len(df)} zones from mart_demand_priority")

    results = run_optimizer(df)
    save_allocations(conn, results)

    print("Allocation decisions saved to public.energy_allocations")
    conn.close()

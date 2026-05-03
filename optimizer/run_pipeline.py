from datetime import datetime
import psycopg2
from energy_optimizer import (
    get_connection, fetch_zone_demand,
    run_optimizer, save_allocations
)
from transport_optimizer import (
    fetch_sources, fetch_allocations,
    run_transport_optimizer, save_flows
)

def main():
    print(f"\n{'#'*65}")
    print(f"  ENERGY OPTIMIZATION PIPELINE")
    print(f"  Run Time: {datetime.utcnow().isoformat()}")
    print(f"{'#'*65}")

    conn = get_connection()

    # ── Stage 1: Fairness Filter (LP) ───────────────────
    print("\n>>> STAGE 1: Priority-Based Allocation (LP)")
    df_demand  = fetch_zone_demand(conn)
    lp_results = run_optimizer(df_demand)
    save_allocations(conn, lp_results)
    print("Stage 1 complete. Allocations saved.")

    # ── Stage 2: Routing Optimizer (Transportation) ──────
    print("\n>>> STAGE 2: Source-to-Zone Routing (Transportation)")
    sources      = fetch_sources(conn)
    allocations  = fetch_allocations(conn)
    tp_results   = run_transport_optimizer(sources, allocations)
    save_flows(conn, tp_results)
    print("Stage 2 complete. Transport flows saved.")

    conn.close()
    print(f"\n{'#'*65}")
    print(f"  Pipeline complete.")
    print(f"{'#'*65}\n")

if __name__ == "__main__":
    main()

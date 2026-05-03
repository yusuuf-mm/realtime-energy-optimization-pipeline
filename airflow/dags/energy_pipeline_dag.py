from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import sys

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner":            "energy_pipeline",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

# ── DB Check ─────────────────────────────────────────────
def check_new_data(**context):
    """
    Checks if new energy readings arrived in the last 15 minutes.
    Fails the task if no data — prevents optimizer running on stale data.
    """
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="energy_db",
        user="energy_user",
        password="energy_pass"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM public.energy_readings
        WHERE ingested_at >= NOW() - INTERVAL '15 minutes'
    """)
    count = cursor.fetchone()[0]
    conn.close()

    print(f"New readings in last 15 minutes: {count}")

    if count == 0:
        raise ValueError("No new data found — skipping pipeline run")

    return count

# ── Optimizer ────────────────────────────────────────────
def run_optimizer(**context):
    sys.path.insert(0, "/opt/airflow/optimizer")
    from energy_optimizer import get_connection, fetch_zone_demand, run_optimizer as lp_run, save_allocations
    from transport_optimizer import fetch_sources, fetch_allocations, run_transport_optimizer, save_flows

    conn = get_connection()

    # Stage 1
    df_demand  = fetch_zone_demand(conn)
    lp_results = lp_run(df_demand)
    save_allocations(conn, lp_results)

    # Stage 2
    sources     = fetch_sources(conn)
    allocations = fetch_allocations(conn)
    tp_results  = run_transport_optimizer(sources, allocations)
    save_flows(conn, tp_results)

    conn.close()
    print("Optimizer pipeline complete")

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="energy_pipeline",
    description="End-to-end energy data pipeline: dbt → optimize",
    schedule="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["energy", "optimization", "dbt"],
) as dag:

    # Task 1: Check new data exists
    t1_check = PythonOperator(
        task_id="check_new_data",
        python_callable=check_new_data,
    )

    # Task 2: Run dbt models
    t2_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="""
            pip install dbt-postgres -q && \
            dbt run \
              --project-dir /opt/airflow/dbt \
              --profiles-dir /opt/airflow/dbt/profiles \
              --log-path /tmp/dbt_logs \
              --target-path /tmp/dbt_target
        """,
    )
    # Task 3: Run two-stage optimizer
    t3_optimize = PythonOperator(
        task_id="run_optimizer",
        python_callable=run_optimizer,
    )

    # ── Task Dependencies ────────────────────────────────
    t1_check >> t2_dbt >> t3_optimize

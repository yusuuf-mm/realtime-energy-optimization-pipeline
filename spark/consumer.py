import json
import boto3
import pandas as pd
import psycopg2
import io
from datetime import datetime
from kafka import KafkaConsumer

# ── Config ───────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC        = "energy-readings"
GROUP_ID     = "energy-spark-consumer"
S3_BUCKET    = "energy-data-lake-yusuufdevops"
S3_PREFIX    = "raw/energy_readings"
BATCH_SIZE   = 10  # upload to S3 every 10 records

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "energy_db",
    "user":     "energy_user",
    "password": "energy_pass"
}

# ── Clients ──────────────────────────────────────────────
s3 = boto3.client("s3", region_name="us-east-1")

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# ── Enrichment ───────────────────────────────────────────
def enrich(record: dict) -> dict:
    record["apparent_power_kva"] = round(
        (record["voltage"] * record["current_amperes"]) / 1000, 3
    )
    return record

# ── Postgres Insert ──────────────────────────────────────
def insert_record(cursor, record: dict):
    cursor.execute("""
        INSERT INTO energy_readings (
            zone, timestamp, energy_consumed_kwh, voltage,
            current_amperes, power_factor, grid_frequency_hz,
            demand_priority, apparent_power_kva
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        record["zone"],
        record["timestamp"],
        record["energy_consumed_kwh"],
        record["voltage"],
        record["current_amperes"],
        record["power_factor"],
        record["grid_frequency_hz"],
        record["demand_priority"],
        record["apparent_power_kva"]
    ))

# ── S3 Upload ────────────────────────────────────────────
def upload_batch_to_s3(batch: list):
    df = pd.DataFrame(batch)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    now = datetime.utcnow()
    key = (
        f"{S3_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{now.strftime('%H%M%S%f')}.parquet"
    )

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  → Uploaded to s3://{S3_BUCKET}/{key}")

# ── Main Consumer Loop ───────────────────────────────────
print(f"Starting consumer → topic: {TOPIC}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest"
)

conn   = get_connection()
cursor = conn.cursor()
batch  = []

for message in consumer:
    record = enrich(message.value)

    # Write to Postgres
    insert_record(cursor, record)
    conn.commit()

    # Accumulate batch
    batch.append(record)

    print(f"[{record['timestamp']}] {record['zone']} → "
          f"{record['energy_consumed_kwh']} kWh | "
          f"{record['apparent_power_kva']} kVA | "
          f"{record['demand_priority']}")

    # Upload to S3 every BATCH_SIZE records
    if len(batch) >= BATCH_SIZE:
        upload_batch_to_s3(batch)
        batch = []

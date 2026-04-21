import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# ── Config ──────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC = "energy-readings"
ZONES = ["Zone-A", "Zone-B", "Zone-C", "Zone-D", "Zone-E"]
INTERVAL_SECONDS = 2

# ── Producer Setup ───────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ── Data Generator ───────────────────────────────────────
def generate_reading(zone: str) -> dict:
    return {
        "zone": zone,
        "timestamp": datetime.utcnow().isoformat(),
        "energy_consumed_kwh": round(random.uniform(10.0, 500.0), 2),
        "voltage": round(random.uniform(210.0, 240.0), 2),
        "current_amperes": round(random.uniform(5.0, 50.0), 2),
        "power_factor": round(random.uniform(0.7, 1.0), 3),
        "grid_frequency_hz": round(random.uniform(49.8, 50.2), 2),
        "demand_priority": random.choice(["low", "medium", "high"]),
    }

# ── Main Loop ────────────────────────────────────────────
print(f"Starting IoT simulator → topic: {TOPIC}")

while True:
    for zone in ZONES:
        reading = generate_reading(zone)
        producer.send(TOPIC, value=reading)
        print(f"[{reading['timestamp']}] {zone} → {reading['energy_consumed_kwh']} kWh")
    
    producer.flush()
    time.sleep(INTERVAL_SECONDS)

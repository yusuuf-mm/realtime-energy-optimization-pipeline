-- Latest snapshot per zone (what the optimizer reads in real-time)
WITH latest AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY zone
            ORDER BY reading_time DESC
        ) AS rn
    FROM {{ ref('stg_energy_readings') }}
)
SELECT
    zone,
    reading_time,
    energy_consumed_kwh,
    active_power_kw,
    apparent_power_kva,
    demand_priority,
    priority_score,
    avg_power_factor,
    voltage
FROM (
    SELECT *,
        AVG(power_factor) OVER (PARTITION BY zone) AS avg_power_factor
    FROM latest
) ranked
WHERE rn = 1
ORDER BY priority_score DESC
-- Cleans and standardizes raw energy readings
SELECT
    id,
    zone,
    timestamp::timestamp                    AS reading_time,
    energy_consumed_kwh,
    voltage,
    current_amperes,
    power_factor,
    grid_frequency_hz,
    apparent_power_kva,
    demand_priority,
    ingested_at,
    -- Derived fields
    CASE
        WHEN demand_priority = 'high'   THEN 3
        WHEN demand_priority = 'medium' THEN 2
        ELSE 1
    END                                     AS priority_score,
    ROUND((voltage * current_amperes * power_factor / 1000)::numeric, 3) AS active_power_kw
FROM public.energy_readings
WHERE energy_consumed_kwh > 0
  AND voltage BETWEEN 200 AND 250
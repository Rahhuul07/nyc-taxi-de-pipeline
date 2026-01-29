-- Build/refresh GOLD daily metrics from SILVER clean table (idempotent via MERGE)
USE DATABASE NYC_TAXI;
USE SCHEMA GOLD;

MERGE INTO GOLD.DAILY_METRICS g
USING (
  SELECT
    CAST(pickup_datetime AS DATE) AS service_date,
    COUNT(*) AS trips,
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    SUM(COALESCE(tip_amount, 0)) AS total_tip,
    AVG(COALESCE(trip_distance, 0)) AS avg_distance,
    AVG(COALESCE(total_amount, 0)) AS avg_total_amount,
    CURRENT_TIMESTAMP() AS updated_at
  FROM NYC_TAXI.SILVER.TAXI_TRIPS_CLEAN
  WHERE pickup_datetime IS NOT NULL
  GROUP BY 1
) s
ON g.service_date = s.service_date

WHEN MATCHED THEN UPDATE SET
  trips = s.trips,
  total_fare = s.total_fare,
  total_tip = s.total_tip,
  avg_distance = s.avg_distance,
  avg_total_amount = s.avg_total_amount,
  updated_at = s.updated_at

WHEN NOT MATCHED THEN INSERT (
  service_date, trips, total_fare, total_tip, avg_distance, avg_total_amount, updated_at
) VALUES (
  s.service_date, s.trips, s.total_fare, s.total_tip, s.avg_distance, s.avg_total_amount, s.updated_at
);

-- Quick sanity check
SELECT *
FROM GOLD.DAILY_METRICS
ORDER BY service_date DESC
LIMIT 10;

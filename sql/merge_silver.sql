USE DATABASE NYC_TAXI;
USE SCHEMA SILVER;

MERGE INTO SILVER.TAXI_TRIPS_CLEAN t
USING (
  SELECT
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    source_file,
    COALESCE(ingested_at, CURRENT_TIMESTAMP()) AS ingested_at
  FROM SILVER.TAXI_TRIPS_STAGE
  WHERE trip_id IS NOT NULL
) s
ON t.trip_id = s.trip_id

WHEN MATCHED THEN UPDATE SET
  vendor_id = s.vendor_id,
  pickup_datetime = s.pickup_datetime,
  dropoff_datetime = s.dropoff_datetime,
  passenger_count = s.passenger_count,
  trip_distance = s.trip_distance,
  pu_location_id = s.pu_location_id,
  do_location_id = s.do_location_id,
  fare_amount = s.fare_amount,
  tip_amount = s.tip_amount,
  total_amount = s.total_amount,
  source_file = s.source_file,
  ingested_at = s.ingested_at

WHEN NOT MATCHED THEN INSERT (
  trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
  trip_distance, pu_location_id, do_location_id, fare_amount, tip_amount,
  total_amount, source_file, ingested_at
) VALUES (
  s.trip_id, s.vendor_id, s.pickup_datetime, s.dropoff_datetime, s.passenger_count,
  s.trip_distance, s.pu_location_id, s.do_location_id, s.fare_amount, s.tip_amount,
  s.total_amount, s.source_file, s.ingested_at
);

-- Data quality checks
SELECT
  COUNT(*) AS total_rows,
  COUNT_IF(trip_id IS NULL) AS null_trip_id,
  COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_trip_id
FROM SILVER.TAXI_TRIPS_CLEAN;


USE DATABASE NYC_TAXI;
USE SCHEMA SILVER;

MERGE INTO SILVER.TAXI_TRIPS_CLEAN t
USING (
  SELECT
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    source_file,
    COALESCE(ingested_at, CURRENT_TIMESTAMP()) AS ingested_at
  FROM SILVER.TAXI_TRIPS_STAGE
  WHERE trip_id IS NOT NULL
) s
ON t.trip_id = s.trip_id

WHEN MATCHED THEN UPDATE SET
  vendor_id = s.vendor_id,
  pickup_datetime = s.pickup_datetime,
  dropoff_datetime = s.dropoff_datetime,
  passenger_count = s.passenger_count,
  trip_distance = s.trip_distance,
  pu_location_id = s.pu_location_id,
  do_location_id = s.do_location_id,
  fare_amount = s.fare_amount,
  tip_amount = s.tip_amount,
  total_amount = s.total_amount,
  source_file = s.source_file,
  ingested_at = s.ingested_at

WHEN NOT MATCHED THEN INSERT (
  trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
  trip_distance, pu_location_id, do_location_id, fare_amount, tip_amount,
  total_amount, source_file, ingested_at
) VALUES (
  s.trip_id, s.vendor_id, s.pickup_datetime, s.dropoff_datetime, s.passenger_count,
  s.trip_distance, s.pu_location_id, s.do_location_id, s.fare_amount, s.tip_amount,
  s.total_amount, s.source_file, s.ingested_at
);

-- Data quality checks
SELECT
  COUNT(*) AS total_rows,
  COUNT_IF(trip_id IS NULL) AS null_trip_id,
  COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_trip_id
FROM SILVER.TAXI_TRIPS_CLEAN;

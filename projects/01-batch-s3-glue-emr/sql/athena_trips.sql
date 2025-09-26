CREATE EXTERNAL TABLE IF NOT EXISTS curated_trips (
  trip_id int,
  ts timestamp,
  city string,
  km double,
  fare double,
  fare_per_km double
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://<bucket>/curated/trips/';
-- Then MSCK REPAIR TABLE curated_trips;

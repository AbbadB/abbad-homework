CREATE TABLE abbad.host_activity_reduced (
month INTEGER,
host VARCHAR,
hit_array ARRAY(INTEGER),
unique_visitors_array ARRAY(INTEGER)
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month', 'host']
)
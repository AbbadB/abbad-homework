CREATE TABLE abbad.user_devices_cumulated (
  user_id BIGINT,
  browser_type VARCHAR,
  dates_active ARRAY(date),
  date DATE
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY['date']
)
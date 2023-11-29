CREATE TABLE abbad.user_devices_cumulated (
  user_id BIGINT,
  device_activity_datelist MAP(VARCHAR, ARRAY(DATE)),
  date DATE
) WITH (
  format = 'PARQUET',
  partitioning = ARRAY['date']
)
INSERT INTO
  abbad.user_devices_cumulated
WITH
  yesterday AS (
    SELECT
      user_id,
      browser_type,
      dates_active
    FROM
      abbad.user_devices_cumulated
    WHERE
      CAST(DATE AS DATE) = DATE('2022-12-31')
  ),
  today AS (
    SELECT
      user_id,
      browser_type,
      ARRAY_AGG(DISTINCT (CAST(event_time AS DATE))) AS dates_active
    FROM
      bootcamp.web_events we
      JOIN bootcamp.devices d ON we.device_id = d.device_id
    WHERE
      CAST(event_time AS DATE) = DATE('2023-01-01')
    GROUP BY
      user_id,
      browser_type
  ),
  cumulated AS (
    SELECT
      COALESCE(y.user_id, t.user_id) AS user_id,
      COALESCE(y.browser_type, t.browser_type) AS browser_type,
      CASE
        WHEN y.dates_active IS NULL THEN t.dates_active
        WHEN t.dates_active IS NULL THEN y.dates_active
        ELSE t.dates_active || y.dates_active
      END AS dates_active,
      DATE('2023-01-01') AS DATE
    FROM
      today t
      FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
  )
SELECT
  *
FROM
  cumulated
GROUP BY
  user_id,
  browser_type,
  dates_active,
  DATE
INSERT INTO
  abbad.hosts_cumulated
WITH
  yesterday AS (
    SELECT
      *
    FROM
      abbad.hosts_cumulated
    WHERE
      DATE = DATE('2023-01-01')
  ),
  today AS (
    SELECT
      host || url AS host,
      ARRAY_AGG(DISTINCT CAST(event_time AS DATE)) AS host_activity_datelist
    FROM
      bootcamp.web_events
    WHERE
      DATE_TRUNC('day', event_time) = DATE('2023-01-02')
    GROUP BY
      CONCAT(host, url)
  )
SELECT
  COALESCE(y.host, t.host) AS host,
  CASE
    WHEN y.host_activity_datelist IS NULL THEN t.host_activity_datelist
    WHEN t.host_activity_datelist IS NULL THEN y.host_activity_datelist
    ELSE t.host_activity_datelist || y.host_activity_datelist
  END AS host_activity_datelist,
  DATE('2023-01-02') DATE
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.host = t.host
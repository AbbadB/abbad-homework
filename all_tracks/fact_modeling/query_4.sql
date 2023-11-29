WITH
  unnested_today AS (
    SELECT
      user_id,
      browser_type,
      CAST(activity_date AS DATE) AS activity_date,
      date
    FROM
      (
        SELECT
          user_id,
          browser_type,
          CAST(DATE AS DATE) AS DATE,
          activity_date
        FROM
          abbad.user_devices_cumulated
          CROSS JOIN UNNEST (device_activity_datelist) AS t (browser_type, dates)
          CROSS JOIN UNNEST (dates) AS dt (activity_date)
        WHERE
          DATE = DATE('2023-01-07')
      ) AS unnested_data
  ),
date_list_int AS (
  SELECT 
  user_id, 
  browser_type, 
  CAST(SUM(POW(2, 31 - DATE_DIFF('day', date, activity_date))) AS BIGINT) AS history_int
  FROM unnested_today
  GROUP BY user_id, browser_type
)

SELECT *,
  TO_BASE(history_int, 2) as history_binary,
  BIT_COUNT(history_int, 64) AS num_days_active
FROM date_list_int

WITH
  exploded AS (
    SELECT
      user_id,
      browser_type,
      activity_date,
      DATE
    FROM
      abbad.user_devices_cumulated
      CROSS JOIN UNNEST (dates_active) AS t (activity_date)
    WHERE
      DATE = DATE('2023-01-07')
  ),
  casted_date AS (
    SELECT
      user_id,
      browser_type,
      CAST(
        SUM(
          POW(2, 31 - DATE_DIFF('day', DATE, activity_date))
        ) AS BIGINT
      ) AS history_int
    FROM
      exploded
    GROUP BY
      user_id,
      browser_type
  )
SELECT
  *,
  TO_BASE(history_int, 2) AS history_binary,
  BIT_COUNT(history_int, 64) AS num_days_active
FROM
  casted_date
INSERT INTO abbad.actors_history_scd
WITH lagged AS (
SELECT
  actor,
  actorid,
  is_active,
  LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS is_active_last_year,
  quality_class,
  LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS quality_class_last_year,
  current_year
FROM
  abbad.actors
WHERE current_year <= 2003
),
streaked AS (
SELECT 
  *,
  SUM(CASE
    WHEN is_active <> is_active_last_year THEN 1 ELSE 0
  END) OVER(PARTITION BY actorid ORDER BY current_year) AS streak_active,
  SUM(CASE
    WHEN quality_class <> quality_class_last_year THEN 1 ELSE 0
  END) OVER(PARTITION BY actorid ORDER BY current_year) AS streak_quality_class
FROM lagged 
)

SELECT 
  actor,
  actorid,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2003 AS current_year
FROM streaked
GROUP BY actor, actorid, streak_active, streak_quality_class
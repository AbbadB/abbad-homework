INSERT INTO abbad.actors_history_scd
WITH last_year_scd AS (
  SELECT * FROM abbad.actors_history_scd
  WHERE current_year = 2002
),
current_year_scd AS (
  SELECT * FROM abbad.actors_history_scd
  WHERE current_year = 2003
),
combined AS (
SELECT 
  COALESCE(ly.actor, cy.actor) AS actor,
  COALESCE(ly.actorid, cy.actorid) AS actorid,
  COALESCE(ly.start_date, cy.start_date) AS start_date,
  COALESCE(ly.end_date, cy.end_date) AS end_date,
  ly.quality_class AS quality_class_last_year,
  cy.quality_class AS quality_class_current_year,
  ly.is_active AS is_active_last_year,
  cy.is_active AS is_active_current_year,
  CASE
    WHEN ly.quality_class <> cy.quality_class OR ly.is_active <> cy.is_active THEN 1
    WHEN ly.quality_class = cy.quality_class OR ly.is_active = cy.is_active THEN 0
  END AS did_change,
  2003 AS current_year
FROM last_year_scd ly
FULL OUTER JOIN current_year_scd cy
ON ly.actorid = cy.actorid AND
ly.end_date + 1 = cy.current_year
),
changes AS (
SELECT 
  actor,
  actorid,
  current_year,
  CASE
    WHEN did_change = 0
      THEN ARRAY[
      CAST(ROW(
      quality_class_last_year, 
      is_active_last_year, 
      start_date, end_date + 1
      ) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
      ]
    WHEN did_change = 1
      THEN ARRAY[
        CAST(ROW(quality_class_last_year, is_active_last_year, start_date, end_date) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER)),
        CAST(ROW(quality_class_current_year, is_active_current_year, current_year, current_year) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
        ]
    WHEN did_change IS NULL
      THEN ARRAY[CAST(ROW(
        COALESCE(quality_class_last_year, quality_class_current_year),
        COALESCE(is_active_last_year, is_active_current_year),
        start_date,
        end_date
        ) AS ROW(quality_class VARCHAR, is_active BOOLEAN, start_date INTEGER, end_date INTEGER))
        ]
    END as change_array
FROM combined
)
SELECT 
  actor, 
  actorid,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST(change_array) AS arr
INSERT INTO
  abbad.players_status_tracking
WITH
  yesterday AS (
    SELECT
      *
    FROM
      abbad.players_status_tracking
    WHERE
      YEAR = 2003
  ),
  today AS (
    SELECT
      player_id,
      MAX(ng.season) AS active_season
    FROM
      bootcamp.nba_player_seasons nps
      JOIN bootcamp.nba_game_details ngd ON nps.player_name = ngd.player_name
      JOIN bootcamp.nba_games ng ON ngd.game_id = ng.game_id
    WHERE
      ng.season = 2003
    GROUP BY
      player_id
  ),
  combined AS (
    SELECT
      COALESCE(y.player_id, t.player_id) AS player_id,
      COALESCE(y.first_active_season, t.active_season) AS first_active_season,
      y.last_active_season AS active_season_yesterday,
      t.active_season,
      COALESCE(t.active_season, y.last_active_season) AS last_active_season,
      CASE
        WHEN y.seasons_active IS NULL THEN ARRAY[t.active_season]
        WHEN t.active_season IS NULL THEN y.seasons_active
        ELSE y.seasons_active || ARRAY[t.active_season]
      END AS seasons_active,
      2004 AS partition_season
    FROM
      yesterday y
      FULL OUTER JOIN today t ON t.player_id = y.player_id
  )
SELECT
  player_id,
  first_active_season,
  last_active_season,
  seasons_active,
  CASE
    WHEN active_season - first_active_season = 0 THEN 'New'
    WHEN active_season - last_active_season = 0 THEN 'Continued Playing'
    WHEN active_season - active_season_yesterday > 1 THEN 'Returned from Retirement'
    WHEN active_season IS NULL
    AND partition_season - last_active_season = 1 THEN 'Retired'
    ELSE 'Stayed Retired'
  END AS season_active_state,
  partition_season AS YEAR
FROM
  combined
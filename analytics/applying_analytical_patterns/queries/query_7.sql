WITH
  lebron_score AS (
    SELECT
      game_date_est,
      player_name,
      pts,
      LAG(pts) OVER (
        PARTITION BY
          player_name
        ORDER BY
          game_date_est
      ) AS prev_pts
    FROM
      bootcamp.nba_game_details
      JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
    WHERE
      player_name = 'LeBron James'
      AND nba_games.game_status_text = 'Final'
  )
SELECT
  COUNT(*) AS consecutive_games
FROM
  lebron_score
WHERE
  pts > 10
  AND (
    prev_pts IS NULL
    OR prev_pts > 10
  )
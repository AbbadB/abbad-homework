WITH
  lebron_score AS (
    SELECT
      game_date_est,
      player_name,
      pts,
      CASE
        WHEN pts > 10 THEN 1
        ELSE 0
      END AS score_flag
    FROM
      bootcamp.nba_game_details
      JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
    WHERE
      player_name = 'LeBron James'
      AND nba_games.game_status_text = 'Final'
  ),
  streaks AS (
    SELECT
      *,
      SUM(CASE WHEN score_flag = 0 THEN 1 ELSE 0 END) OVER (
        ORDER BY game_date_est
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS streak_id
    FROM
      lebron_score
  )
SELECT
  streak_id,
  COUNT(*) AS streak_length
FROM
  streaks
WHERE
  score_flag = 1
GROUP BY
  streak_id
ORDER BY
  streak_length DESC

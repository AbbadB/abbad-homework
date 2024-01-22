WITH windowed_data AS (
    SELECT
      team_id,
      game_date_est,
      COUNT(*) OVER (
        PARTITION BY team_id
        ORDER BY game_date_est
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ) AS game_count,
      SUM(
        CASE
          WHEN pts_home > pts_away THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY team_id
        ORDER BY game_date_est
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ) AS wins_in_stretch
    FROM
      bootcamp.nba_game_details
      JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
    WHERE
      nba_games.game_status_text = 'Final'
)
SELECT
  team_id,
  MAX(wins_in_stretch) AS max_wins_in_stretch
FROM
  windowed_data
GROUP BY
  team_id
ORDER BY max_wins_in_stretch DESC
LIMIT 1
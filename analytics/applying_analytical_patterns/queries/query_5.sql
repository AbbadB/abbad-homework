WITH
  team_wins AS (
    SELECT
      COALESCE(team_city, 'overall') AS team_city,
      COALESCE(player_name, 'overall') AS player_name,
      COALESCE(CAST(season AS VARCHAR), 'overall') AS season,
      COUNT(*) AS wins
    FROM
      bootcamp.nba_game_details
      JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
    WHERE
      nba_games.game_status_text = 'Final'
      AND pts_home > pts_away
    GROUP BY
      GROUPING SETS (
        (player_name, team_city),
        (player_name, season),
        (team_city),
        ()
      )
  )
SELECT
  team_city,
  MAX(wins) AS max_wins
FROM
  team_wins
GROUP BY
  team_city
LIMIT
  1
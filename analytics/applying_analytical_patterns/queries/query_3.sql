SELECT
  COALESCE(CAST(team_id AS VARCHAR), 'overall') AS team_id,
  COALESCE(player_name, 'overall') AS player_name,
  COALESCE(CAST(season AS VARCHAR), 'overall') AS season,
  SUM(pts) AS pts
FROM
  bootcamp.nba_game_details
  JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
GROUP BY
  GROUPING SETS (
    (player_name, team_id),
    (player_name, season),
    (team_id),
    ()
  )
HAVING
  CAST(team_id AS VARCHAR) <> 'overall'
  AND player_name <> 'overall'
ORDER BY pts DESC
LIMIT 1

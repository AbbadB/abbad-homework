SELECT
  COALESCE(team_city, 'overall') AS team_city,
  COALESCE(player_name, 'overall') AS player_name,
  COALESCE(CAST(season AS VARCHAR), 'overall') AS season,
  SUM(pts) as pts
FROM
  bootcamp.nba_game_details
  JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
GROUP BY
  GROUPING SETS ((player_name, team_city),
                 (player_name, season),
                 (team_city, season),
                 ())
ORDER BY team_city, player_name, season
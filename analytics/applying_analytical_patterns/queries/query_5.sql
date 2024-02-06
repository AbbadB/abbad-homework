SELECT
  COALESCE(CAST(team_id AS VARCHAR), 'overall') AS team_id,
  COALESCE(player_name, 'overall') AS player_name,
  COALESCE(CAST(season AS VARCHAR), 'overall') AS season,
  SUM(home_team_wins) AS wins
FROM
  bootcamp.nba_game_details
  JOIN bootcamp.nba_games ON nba_game_details.game_id = nba_games.game_id
  AND bootcamp.nba_games.home_team_id = nba_game_details.team_id
GROUP BY
  GROUPING SETS (
    (player_name, team_id),
    (player_name, season),
    (team_id),
    ()
  )
ORDER BY wins DESC
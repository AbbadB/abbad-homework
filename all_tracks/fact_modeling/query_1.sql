WITH fct_nba_game_details AS (
SELECT
  game_id,
  team_id,
  team_abbreviation,
  team_city,
  player_id,
  player_name,
  nickname,
  start_position,
  comment,
  min,
  fgm,
  fga,
  fg3m,
  fg3a,
  ftm,
  fta,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  to,
  pf,
  pts,
  plus_minus,
  ROW_NUMBER() OVER (PARTITION BY player_id, game_id, team_id ORDER BY (SELECT NULL)) AS row_number
FROM bootcamp.nba_game_details
)
SELECT *
FROM fct_nba_game_details 
WHERE row_number = 1
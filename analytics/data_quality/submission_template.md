# League of Legends Player Performance Analysis Pipeline

## Group

- Bruno Abbad
- Ilich Salazar

## Problem Statement

This project aims to develop a data pipeline that aggregates and analyzes data from League of Legends, focusing on Master+ ranked players across all regions. The goal is to predict team win probabilities based on specific champion choices and combinations, as well as player synergies. The insights gleaned will be used to build a real-time analytics dashboard.

## Business Metrics

1. Prediction Accuracy: Accuracy of win probability predictions.
2. Data Freshness: Timeliness of data for real-time analytics.
3. User Engagement: Dashboard usage and interaction metrics.
4. Data Coverage: Completeness of data across regions and player tiers.
5. Power of Champions: How powerful a specific champion is on the current game patch.
6. Most Player Champions: Which champions are more played so can focus on them when creating skins.

## Assumptions

1. Champion selection significantly influences match outcomes.
2. Data from Master+ players is available and consistent across regions.
3. Player combinations affect team performance.
4. Real-time match data can be processed with minimal latency.

## Upstream Sources

1. Riot Games Patch API: For current patch version data.
2. League of Legends Live Client Data API: For real-time match data.
3. Riot Games Match API (match-v5): For historical match data.
4. Riot Games Summoner API (summoner-v4): For player profiles and ranks.
5. Riot Games League API (league-v4): For player standings in Master+ tiers.
6. Riot Games Static Data API: For static data about champions.

## Pipeline Diagram

![alt text](https://files.catbox.moe/fnw7lr.jpg)

## Data Schemas/Modeling

### Fact Table: Match Performance

| Column             | Data Type | Description                                      |
|--------------------|-----------|--------------------------------------------------|
| match_id           | INT       | Unique identifier for the match                  |
| region             | TEXT      | Geographic region of the match                   |
| player_id          | INT       | Unique identifier of the player                  |
| team_id            | INT       | Unique identifier for the team                   |
| champion_id        | INT       | ID of the champion used by the player            |
| win                | BOOLEAN   | Whether the player's team won the match          |
| kills              | INT       | Number of kills by the player                    |
| deaths             | INT       | Number of deaths by the player                   |
| assists            | INT       | Number of assists by the player                  |
| total_damage_dealt | INT       | Total damage dealt by the player                 |
| total_damage_taken | INT       | Total damage taken by the player                 |
| patch              | TEXT      | Patch version of the match                       |

### Fact Table: Match Timeline

| Column            | Data Type | Description                                       |
|-------------------|-----------|---------------------------------------------------|
| match_id          | INT       | Unique identifier for the match                   |
| timestamp         | INT       | Time elapsed in the match (in seconds)            |
| event_id          | INT       | Unique identifier for the event                   |
| event_type        | TEXT      | Type of event (kill, objective, etc.)             |
| involved_player_id| INT       | ID of the player involved in the event            |
| team_id           | INT       | Team ID of the involved player                    |
| event_details     | TEXT      | Additional details about the event                |

### Fact Table: Player Combinations

| Column            | Data Type | Description                                        |
|-------------------|-----------|----------------------------------------------------|
| match_id          | INT       | Unique identifier for the match                    |
| player_1_id       | INT       | Unique identifier of the first player              |
| player_2_id       | INT       | Unique identifier of the second player             |
| ...               |           | (continued for all 10 players)                     |
| player_10_id      | INT       | Unique identifier of the tenth player              |
| team_1_id         | INT       | Unique identifier for the first team               |
| team_2_id         | INT       | Unique identifier for the second team              |
| outcome_team_1    | BOOLEAN   | Outcome of the match for the first team (win/lose) |
| outcome_team_2    | BOOLEAN   | Outcome of the match for the second team (win/lose)|

### Dimension Table: Champions

| Column          | Data Type | Description                                 |
|-----------------|-----------|---------------------------------------------|
| champion_id     | INT       | Unique identifier for the champion          |
| name            | TEXT      | Name of the champion                        |
| role            | TEXT      | Primary role of the champion                |
| release_date    | DATE      | Release date of the champion                |
| patch_introduced| TEXT      | Patch when the champion was introduced      |

### Dimension Table: Players

| Column        | Data Type | Description                                  |
|---------------|-----------|----------------------------------------------|
| player_id     | INT       | Unique identifier of the player              |
| summoner_name | TEXT      | In-game name of the player                   |
| rank          | TEXT      | Rank of the player                           |
| region        | TEXT      | Geographic region of the player              |
| role          | TEXT      | Preferred role of the player in the game     |

## Data Quality Checks

### Match Performance Table

| Column             | Data Quality Check                                  |
|--------------------|-----------------------------------------------------|
| match_id           | Ensure values are non-null and unique.              |
| region             | Ensure values are non-null and within expected list.|
| player_id          | Ensure values are non-null and correspond to a valid player.|
| team_id            | Ensure values are non-null and valid.               |
| champion_id        | Ensure values are non-null and correspond to a valid champion.|
| win                | Ensure values are non-null and Boolean.             |
| kills              | Ensure values are non-negative integers.            |
| deaths             | Ensure values are non-negative integers.            |
| assists            | Ensure values are non-negative integers.            |
| total_damage_dealt | Ensure values are non-negative integers.            |
| total_damage_taken | Ensure values are non-negative integers.            |
| patch              | Ensure values are non-null and match known patches. |

### Match Timeline Table

| Column            | Data Quality Check                                  |
|-------------------|-----------------------------------------------------|
| match_id          | Ensure values are non-null and unique.              |
| timestamp         | Ensure values are non-negative integers.            |
| event_id          | Ensure values are non-null and unique.              |
| event_type        | Ensure non-null and within expected list of event types. |
| involved_player_id| Ensure values are non-null and correspond to valid player IDs. |
| team_id           | Ensure values are non-null and valid.               |
| event_details     | Ensure values are non-null and valid strings.       |

### Player Combinations Table

| Column            | Data Quality Check                                  |
|-------------------|-----------------------------------------------------|
| match_id          | Ensure values are non-null and unique.              |
| player_1_id       | Ensure values are non-null and correspond to valid player IDs.|
| player_2_id       | Ensure values are non-null and correspond to valid player IDs.|
| ...               | (continued for all player IDs)                      |
| player_10_id      | Ensure values are non-null and correspond to valid player IDs.|
| team_1_id         | Ensure values are non-null and valid.               |
| team_2_id         | Ensure values are non-null and valid.               |
| outcome_team_1    | Ensure values are non-null and Boolean.             |
| outcome_team_2    | Ensure values are non-null and Boolean.             |

### Champions Dimension Table

| Column          | Data Quality Check                              |
|-----------------|-------------------------------------------------|
| champion_id     | Ensure values are non-null and unique.          |
| name            | Ensure values are non-null and valid strings.   |
| role            | Ensure values are non-null and within expected list of roles. |
| release_date    | Ensure values are non-null and in YYYY-MM-DD format. |
| patch_introduced| Ensure values are non-null and match known patches. |

### Players Dimension Table

| Column        | Data Quality Check                                  |
|---------------|-----------------------------------------------------|
| player_id     | Ensure values are non-null and unique.              |
| summoner_name | Ensure values are non-null and valid strings.       |
| rank          | Ensure values are non-null and within expected rank tiers.|
| region        | Ensure values are non-null and within expected list.|
| role          | Ensure values are non-null and within expected list of roles. |
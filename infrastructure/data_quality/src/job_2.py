from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    return f"""
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
    """

def job_2(spark, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark.sql(query_2(output_table_name))

def main():
    output_table_name: str = "lebron_score"
    spark = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    return f"""
    WITH lagged AS (
    SELECT
        actor,
        actorid,
        is_active,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS is_active_last_year,
        quality_class,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS quality_class_last_year,
        current_year
    FROM
        abbad.actors
    WHERE current_year <= 2003
),
streaked AS (
    SELECT 
        *,
        SUM(CASE
            WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1 ELSE 0
        END) OVER(PARTITION BY actorid ORDER BY current_year) AS streak
    FROM lagged 
)

SELECT 
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2003 AS current_year
FROM streaked
GROUP BY actor, actorid, quality_class, is_active, streak

    """

def job_1(spark, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark.sql(query_1(output_table_name))

def main():
    output_table_name: str = "actors_history_scd"
    spark = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
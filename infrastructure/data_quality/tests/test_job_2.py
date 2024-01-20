from chispa.dataframe_comparer import *
from src.job_2 import job_2
from collections import namedtuple
lebronScore = namedtuple("LebronScore", "consecutive_games")
lebronTable = namedtuple("LebronTable", "consecutive_games")


def test_scd_generation(spark):

    

    source_data = [
        lebronScore(1668),
    ]
    source_df = spark.createDataFrame(source_data)

    temp_table_name = "temp_lebron"
    source_df.createOrReplaceTempView(temp_table_name)

    actual_df = job_2(spark, temp_table_name)
    expected_data = [
        lebronTable(1668),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
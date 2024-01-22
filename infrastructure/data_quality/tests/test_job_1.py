from chispa.dataframe_comparer import *
from src.job_1 import job_1
from collections import namedtuple
ActorYear = namedtuple("ActorYear", "actor actorid quality_class is_active start_date end_date current_year")
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_date end_date current_year")


def test_scd_generation(spark):

    

    source_data = [
        ActorYear("Ted Raimi", "nm0001646", "good", True, 2001, 2001, 2001),
        ActorYear("Treat Williams", "nm0001852", "good", True, 2001, 2001, 2001),
        ActorYear("Mathieu Amalric", "nm0023832", "good", True, 2000, 2000, 2001),
        ActorYear("Mathieu Amalric", "nm0023832", "good", False, 2001, 2001, 2001),
        ActorYear("Art Malik", "nm0539562", "good", True, 2001, 2001, 2001),
    ]
    source_df = spark.createDataFrame(source_data)

    temp_table_name = "temp_actors"
    source_df.createOrReplaceTempView(temp_table_name)

    actual_df = job_1(spark, temp_table_name)
    expected_data = [
        ActorScd("Ted Raimi", "nm0001646", "good", True, 2001, 2001, 2001),
        ActorScd("Treat Williams", "nm0001852", "good", True, 2001, 2001, 2001),
        ActorScd("Mathieu Amalric", "nm0023832", "good", True, 2000, 2000, 2001),
        ActorScd("Mathieu Amalric", "nm0023832", "good", False, 2001, 2001, 2001),
        ActorScd("Art Malik", "nm0539562", "good", True, 2001, 2001, 2001),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
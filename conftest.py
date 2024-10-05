import pytest
from lib.Utils import get_spark_session
@pytest.fixture
def spark():
    spark_session= get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()
@pytest.fixture
def expected_results(spark):
    schema="state string count int"
    return spark.read.format("csv").schema(schema).load("data/test_result/aggregate.csv")
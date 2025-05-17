"""
    Contains shared pytest fixtures for the rest of the tests.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.config.consts import ACTIONS, CONDITIONS, RULE_NAME


@pytest.fixture(name="spark", scope="session")
def fixture_spark():
    """
    Create a Spark session for testing.
    """
    spark = SparkSession.builder.appName("Test Spark Session").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(name="sample_df")
def fixture_sample_df(spark):
    """
    Create a sample DataFrame for testing.
    """
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [(1, "a"), (2, "b"), (3, "c")]
    return spark.createDataFrame(data, schema)


@pytest.fixture(name="sample_rule_config")
def fixture_sample_rule_config():
    """
    Provides a sample rule configuration for testing.
    """
    return {
        RULE_NAME: "rule1",
        CONDITIONS: [{"expression": "id == 1"}],
        ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
    }

"""
    Unit tests for the Action class.
"""

import pyspark
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.utils import assertDataFrameEqual

from src.action import Action, ActionConfig
from src.config.consts import OPERATION, OTHERWISE, OUTPUT_COL_NAME
from src.enums.rule_type_enum import ExecutionType


class TestAction:
    """
    This class contains unit tests to validate the behavior of the Action class.
    """

    def test_action_config_creation(self):
        """
        Test the creation of an ActionConfig object.

        Validates that the ActionConfig object is correctly initialized with the
        provided configuration data.
        """
        # Given
        config_data = {
            OUTPUT_COL_NAME: "new_col",
            OPERATION: "value + '1'",
            OTHERWISE: "'default'",
        }

        # When
        action_config = ActionConfig(**config_data)

        # Then
        assert action_config.output_col_name == "new_col"
        assert action_config.operation == "value + '1'"
        assert action_config.otherwise == "'default'"

    def test_action_creation(self):
        """
        Test the creation of an Action object.

        Ensures that the Action object is correctly initialized.
        """
        # Given
        config_data = {
            OUTPUT_COL_NAME: "new_col",
            OPERATION: "value + '1'",
            OTHERWISE: "'default'",
        }
        action_config = ActionConfig(**config_data)
        rule_name = "test_rule"

        # When
        action = Action(action_config, rule_name, ExecutionType.ACCUMULATIVE)

        # Then
        assert action.output_col_name == "new_col"
        assert action.operation == "value + '1'"
        assert action.otherwise == "'default'"
        assert action.rule_name == rule_name

    def test_execute_happy_path(self, spark, sample_df):
        """
        Test the execution of an Action object under normal conditions.

        Validates that the action is applied correctly when conditions are met.
        """
        # Given
        config_data = {
            OUTPUT_COL_NAME: "new_col",
            OPERATION: "value || '1'",
            OTHERWISE: "'default'",
        }
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "default", "default"),
            (2, "b", "b1", "b1"),
            (3, "c", "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_no_otherwise(self, spark, sample_df):
        """
        Test the execution of an Action object without an "otherwise" clause.

        Ensures that the output column is set to None when conditions are not met.
        """
        # Given
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", None, None),
            (2, "b", "b1", "b1"),
            (3, "c", "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_existing_column_with_otherwise(self, spark, sample_df):
        """
        Test the execution of an Action object when the output column already exists.

        Validates that the "otherwise" clause is applied correctly.
        """
        # Given
        sample_df = sample_df.withColumn("new_col", lit("initial"))
        config_data = {
            OUTPUT_COL_NAME: "new_col",
            OPERATION: "value || '1'",
            OTHERWISE: "'default'",
        }
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "default", "default"),
            (2, "b", "b1", "b1"),
            (3, "c", "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_existing_column_no_otherwise(self, spark, sample_df):
        """
        Test the execution of an Action object when the output column already exists
        and no "otherwise" clause is provided.

        Ensures that the existing column values are preserved when conditions are not met.
        """
        # Given
        sample_df = sample_df.withColumn("new_col", lit("initial"))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "initial", "initial"),
            (2, "b", "b1", "b1"),
            (3, "c", "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_single_rule_blocked(self, spark, sample_df):
        """
        Test the execution of a cascade rule when it is blocked by a previous rule.

        Ensures that the action is not applied when the cascade condition is blocked.
        """
        # Given
        sample_df = sample_df.withColumn("prev_rule1", lit(True))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(True)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", True, None, None),
            (2, "b", True, None, None),
            (3, "c", True, None, None),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_single_rule_executed_fully(self, spark, sample_df):
        """
        Test the execution of a cascade rule when it is not blocked by any previous rule.

        Validates that the action is applied correctly.
        """
        # Given
        sample_df = sample_df.withColumn("prev_rule1", lit(False))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", False, None, None),
            (2, "b", False, "b1", "b1"),
            (3, "c", False, "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_single_rule_blocked_existing_column(
        self, spark, sample_df
    ):
        """
        Test the execution of a cascade rule when it is blocked and the output column
        already exists.

        Ensures that the existing column values are preserved.
        """
        # Given
        sample_df = sample_df.withColumn("new_col", lit("initial"))
        sample_df = sample_df.withColumn("prev_rule1", lit(True))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(True)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "initial", True, "initial"),
            (2, "b", "initial", True, "initial"),
            (3, "c", "initial", True, "initial"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_single_rule_executed_fully_existing_column(
        self, spark, sample_df
    ):
        """
        Test the execution of a cascade rule when it is not blocked, the output column
        already exists, and the action is applied correctly.

        Ensures that the action is applied to the existing column.
        """
        # Given
        sample_df = sample_df.withColumn("new_col", lit("initial"))
        sample_df = sample_df.withColumn("prev_rule1", lit(False))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "initial", False, "initial"),
            (2, "b", "b1", False, "b1"),
            (3, "c", "c1", False, "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_single_rule_executed_fully_existing_column_with_otherwise(
        self, spark, sample_df
    ):
        """
        Test the execution of a cascade rule when it is not blocked, the output column
        already exists, and an "otherwise" clause is provided.

        Ensures that the "otherwise" clause is applied correctly.
        """
        # Given
        sample_df = sample_df.withColumn("new_col", lit("initial"))
        sample_df = sample_df.withColumn("prev_rule1", lit(False))
        config_data = {
            OUTPUT_COL_NAME: "new_col",
            OPERATION: "value || '1'",
            OTHERWISE: "'default'",
        }
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", "default", False, "default"),
            (2, "b", "b1", False, "b1"),
            (3, "c", "c1", False, "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_multiple_rule_blocked(self, spark, sample_df):
        """
        Test the execution of a cascade rule when it is blocked by multiple previous rules.

        Ensures that the action is not applied when any cascade condition is blocked.
        """
        # Given
        sample_df = sample_df.withColumn("prev_rule1", lit(True))
        sample_df = sample_df.withColumn("prev_rule2", lit(False))
        sample_df = sample_df.withColumn("prev_rule3", lit(True))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(True)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", True, False, True, None, None),
            (2, "b", True, False, True, None, None),
            (3, "c", True, False, True, None, None),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("prev_rule2", BooleanType(), False),
                StructField("prev_rule3", BooleanType(), False),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_execute_cascade_multiple_rule_executed_fully(self, spark, sample_df):
        """
        Test the execution of a cascade rule when it is not blocked by any previous rules.

        Validates that the action is applied correctly.
        """
        # Given
        sample_df = sample_df.withColumn("prev_rule1", lit(False))
        sample_df = sample_df.withColumn("prev_rule2", lit(False))
        sample_df = sample_df.withColumn("prev_rule3", lit(False))
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.CASCADE)
        conditions = col("id") > 1
        cascade_blocked = pyspark.sql.functions.lit(False)

        # When
        result_df = action.execute(sample_df, conditions, cascade_blocked)

        # Then
        expected_data = [
            (1, "a", False, False, False, None, None),
            (2, "b", False, False, False, "b1", "b1"),
            (3, "c", False, False, False, "c1", "c1"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("prev_rule1", BooleanType(), False),
                StructField("prev_rule2", BooleanType(), False),
                StructField("prev_rule3", BooleanType(), False),
                StructField("new_col", StringType(), True),
                StructField("test_rule_new_col", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result_df, expected_df)

    def test_get_history_column_name(self):
        """
        Test the generation of the history column name.

        Ensures that the history column name is correctly formatted based on the
        rule name and output column name.
        """
        # Given
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)

        # When
        history_column_name = action.get_history_column_name()

        # Then
        assert history_column_name == "test_rule_new_col"

    def test_str_representation(self):
        """
        Test the string representation of an Action object.

        Validates that the string representation is correctly formatted.
        """
        # Given
        config_data = {OUTPUT_COL_NAME: "new_col", OPERATION: "value || '1'"}
        action_config = ActionConfig(**config_data)
        action = Action(action_config, "test_rule", ExecutionType.ACCUMULATIVE)

        # When
        str_representation = str(action)

        # Then
        assert str_representation == "new_col = value || '1'"

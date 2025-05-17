"""
    Unit tests for the Condition class.
"""

from src.condition import Condition, ConditionConfig
from src.config.consts import EXPRESSION


class TestCondition:
    """
    This class contains unit tests to validate the behavior of the Condition class.
    """

    def test_evaluate(self, spark, sample_df):
        """
        Test the evaluation of a Condition object.

        Validates that the condition is correctly evaluated as a PySpark column
        and applied to a sample DataFrame.
        """
        # Given
        # Create a condition that checks if id == 1
        condition_config = ConditionConfig(expression="id == 1")
        condition = Condition(condition_config)

        # When
        # Evaluate the condition (should return a pyspark.sql.Column)
        result_column = condition.evaluate()

        # Apply the condition to the sample DataFrame
        df_result = sample_df.withColumn("condition_result", result_column)

        # Collect results to verify correctness
        results = df_result.select("id", "condition_result").collect()

        # Then
        for row in results:
            expected_value = row["id"] == 1  # True if id == 1, else False
            assert row["condition_result"] == expected_value

    def test_str_representation(self):
        """
        Test the string representation of a Condition object.

        Validates that the string representation matches the condition's expression.
        """
        # Given
        config_data = {EXPRESSION: "transaction_amount > 60"}
        condition_config = ConditionConfig(**config_data)
        condition = Condition(condition_config)

        # When
        str_representation = str(condition)

        # Then
        assert str_representation == "transaction_amount > 60"

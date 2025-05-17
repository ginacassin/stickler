"""
    Unit tests for the ReferenceValidator class.
"""

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.config.consts import ACTIONS, CONDITIONS, RULE_NAME
from src.rule import Rule, RuleConfig
from src.utils.validator.reference_validator import ReferenceValidator


class TestReferenceValidator:
    """
    This class contains unit tests to validate the behavior of the ReferenceValidator class.
    """

    def test_single_rule_correct(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a single rule with correct references.

        Validates that the ReferenceValidator does not raise an exception for a rule
        with valid column and UDF references.
        """
        # Given
        rule_config = RuleConfig(**sample_rule_config)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then this should not raise an exception
        reference_validator.validate(rule, sample_df)

    def test_extract_columns(self):
        """
        Test extraction of column references from expressions.

        Validates that the ReferenceValidator correctly extracts column names
        from various expressions.
        """
        # Given
        reference_validator = ReferenceValidator()

        # Then
        # Check that 'id' is included in the columns
        assert reference_validator.extract_columns("id == 1") == ["id"]
        # Check that 'a' is not included in the columns
        assert reference_validator.extract_columns("id == 1 AND value == 'a'") == [
            "id",
            "value",
        ]
        assert reference_validator.extract_columns("id == 1 OR value == 'a'") == [
            "id",
            "value",
        ]
        assert reference_validator.extract_columns(
            "id == 1 AND (value == 'a' OR value == 'b')"
        ) == ["id", "value"]
        # Check that 'a' IS included in the columns, even though it's also a string
        assert reference_validator.extract_columns("id == 1 OR a == 'a'") == ["id", "a"]

    def test_non_existing_column(self, spark, sample_df):
        """
        Test validation of a rule with a non-existing column reference.

        Validates that the ReferenceValidator raises a ValueError for a rule
        with a column reference that does not exist in the DataFrame.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "non_existing_column == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="doesn't exist"):
            reference_validator.validate(rule, sample_df)

    def test_column_defined_in_one_rule_and_used_in_another(
        self, spark, sample_df, sample_rule_config
    ):
        """
        Test validation of a column defined in one rule and used in another.

        Validates that the ReferenceValidator does not raise an exception for a column
        that is defined in one rule and used in subsequent rules.

        Reuses new_column from the first rule in the second rule.
        """
        # Given
        rule_cfg2 = {
            RULE_NAME: "rule2",
            CONDITIONS: [{"expression": "new_column == 2"}],
            ACTIONS: [
                {"output_col_name": "another_new_column", "operation": "new_column + 1"}
            ],
        }
        rule_config1 = RuleConfig(**sample_rule_config)
        rule_config2 = RuleConfig(**rule_cfg2)
        rule1 = Rule(rule_config1)
        rule2 = Rule(rule_config2)

        # When
        reference_validator = ReferenceValidator()

        # Then this should not raise an exception
        reference_validator.validate(rule1, sample_df)
        reference_validator.validate(rule2, sample_df)

    def test_column_defined_in_one_action_and_used_in_another(self, spark, sample_df):
        """
        Test validation of a column defined in one action and used in another action.

        Validates that the ReferenceValidator does not raise an exception for a column
        that is defined in one action and used in subsequent actions within the same rule.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [
                {"output_col_name": "new_column", "operation": "value + 1"},
                {
                    "output_col_name": "another_new_column",
                    "operation": "new_column + 1",
                },
            ],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then this should not raise an exception
        reference_validator.validate(rule, sample_df)

    def test_condition_references_column_defined_in_same_rule(self, spark, sample_df):
        """
        Test validation of a condition referencing a column defined in the same rule.

        Validates that the ReferenceValidator raises a ValueError for a condition
        that references a column defined in the same rule.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "new_column == 2"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="doesn't exist"):
            reference_validator.validate(rule, sample_df)

    def test_non_existing_udf_in_action(self, spark, sample_df):
        """
        Test validation of a rule with a non-existing UDF in an action.

        Validates that the ReferenceValidator raises a ValueError for a rule
        with a UDF reference that does not exist in the Spark session.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "udf(value) + 1"}],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="doesn't exist"):
            reference_validator.validate(rule, sample_df)

    def test_existing_udf_in_action(self, spark, sample_df):
        """
        Test validation of a rule with an existing UDF in an action.

        Validates that the ReferenceValidator does not raise an exception for a rule
        with a UDF reference that exists in the Spark session.
        """
        # Given a simple UDF
        def my_udf(value):
            return value + "_udf"

        my_udf_spark = udf(my_udf, StringType())

        # Register the UDF
        spark.udf.register("my_udf", my_udf_spark)

        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [
                {"output_col_name": "new_column", "operation": "my_udf(value) + 1"}
            ],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        reference_validator = ReferenceValidator()

        # Then this should not raise an exception
        reference_validator.validate(rule, sample_df)

"""
    Unit tests for the ExpressionValidator class.
"""

import pytest

from src.config.consts import ACTIONS, CONDITIONS, RULE_NAME
from src.rule import Rule, RuleConfig
from src.utils.validator.expression_validator import ExpressionValidator


class TestExpressionValidator:
    """
    This class contains unit tests to validate the behavior of the ExpressionValidator class.
    """

    def test_single_rule_correct(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a single rule with correct expressions.

        Validates that the ExpressionValidator does not raise an exception for a rule
        with valid condition and action expressions.
        """
        # Given
        rule_config = RuleConfig(**sample_rule_config)
        rule = Rule(rule_config)

        # When
        expression_validator = ExpressionValidator()

        # Then this should not raise an exception
        expression_validator.validate(rule, sample_df)

    def test_invalid_condition_expression(self, spark, sample_df):
        """
        Test validation of a rule with an invalid condition expression.

        Validates that the ExpressionValidator raises a ValueError for a rule
        with an invalid condition expression.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "1 >> 2"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_config = RuleConfig(**rule_cfg)

        rule = Rule(rule_config)
        # When
        expression_validator = ExpressionValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="Please use a valid operator and syntax"):
            expression_validator.validate(rule, sample_df)

    def test_invalid_action_expression(self, spark, sample_df):
        """
        Test validation of a rule with an invalid action expression.

        Validates that the ExpressionValidator raises a ValueError for a rule
        with an invalid action expression.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule1",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "1 >> 2"}],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        expression_validator = ExpressionValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="Please use a valid operator and syntax"):
            expression_validator.validate(rule, sample_df)

    def test_rule_with_new_column(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a rule that creates a new column.

        Validates that the ExpressionValidator does not raise an exception for a rule
        that creates a new column and uses it in subsequent rules.
        """
        # Given
        rule_config_1 = RuleConfig(**sample_rule_config)
        rule_1 = Rule(rule_config_1)

        rule_cfg_2 = {
            RULE_NAME: "rule2",
            CONDITIONS: [{"expression": "new_column > 2"}],
            ACTIONS: [
                {"output_col_name": "new_column_2", "operation": "new_column + 1"}
            ],
        }
        rule_config_2 = RuleConfig(**rule_cfg_2)
        rule_2 = Rule(rule_config_2)

        # When
        expression_validator = ExpressionValidator()

        # Then this should not raise an exception
        expression_validator.validate(rule_1, sample_df)
        expression_validator.validate(rule_2, sample_df)

    def test_rule_with_new_column_invalid_expression(
        self, spark, sample_df, sample_rule_config
    ):
        """
        Test validation of a rule that creates a new column with an invalid expression.

        Validates that the ExpressionValidator raises a ValueError for a rule
        that creates a new column with an invalid expression.
        """
        # Given
        rule_config_1 = RuleConfig(**sample_rule_config)
        rule_1 = Rule(rule_config_1)

        rule_cfg_2 = {
            RULE_NAME: "rule2",
            CONDITIONS: [{"expression": "new_column > 2"}],
            ACTIONS: [
                {"output_col_name": "new_column_2", "operation": "new_column >> 2"}
            ],
        }
        rule_config_2 = RuleConfig(**rule_cfg_2)
        rule_2 = Rule(rule_config_2)

        # When
        expression_validator = ExpressionValidator()

        # Then this should not raise an exception
        expression_validator.validate(rule_1, sample_df)
        with pytest.raises(ValueError, match="Please use a valid operator and syntax"):
            # But this SHOULD raise an exception
            expression_validator.validate(rule_2, sample_df)

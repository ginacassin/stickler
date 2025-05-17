"""
    Unit tests for the NameValidator class.
"""

import pytest

from src.config.consts import ACTIONS, CONDITIONS, RULE_NAME
from src.rule import Rule, RuleConfig
from src.utils.validator.name_validator import NameValidator


class TestNameValidator:
    """
    This class contains unit tests to validate the behavior of the NameValidator class.
    """

    def test_single_rule_correct(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a single rule with a correct name.

        Validates that the NameValidator does not raise an exception for a rule
        with a valid name and unique column names.
        """
        rule_config = RuleConfig(**sample_rule_config)
        rule = Rule(rule_config)

        # When
        name_validator = NameValidator()

        # Then this should not raise an exception
        name_validator.validate(rule, sample_df)

    def test_single_rule_empty_name(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a rule with an empty name.

        Validates that the NameValidator raises a ValueError for a rule
        with an empty name.
        """
        # Given
        sample_rule_config[RULE_NAME] = ""
        rule_config = RuleConfig(**sample_rule_config)
        rule = Rule(rule_config)

        # When
        name_validator = NameValidator()

        # Then it SHOULD raise an exception with a message indicating the error
        with pytest.raises(ValueError, match="empty"):
            name_validator.validate(rule, sample_df)

    def test_single_rule_name_already_in_df(self, spark, sample_df, sample_rule_config):
        """
        Test validation of a rule with a name that already exists in the DataFrame.

        Validates that the NameValidator raises a ValueError for a rule
        with a name that conflicts with an existing column name in the DataFrame.
        """
        # Given
        sample_rule_config[RULE_NAME] = "value"
        rule_config = RuleConfig(**sample_rule_config)
        rule = Rule(rule_config)

        # When
        name_validator = NameValidator()

        # Then this SHOULD raise an exception with a specific message
        with pytest.raises(ValueError, match="name is the same as a column"):
            name_validator.validate(rule, sample_df)

    def test_rule_name_duplicated(self, spark, sample_df, sample_rule_config):
        """
        Test validation of duplicate rule names.

        Validates that the NameValidator raises a ValueError for rules
        with duplicate names.
        """
        # Given a second rule with the same name as the first one
        rule_config1 = RuleConfig(**sample_rule_config)
        rule_config2 = RuleConfig(**sample_rule_config)
        rule1 = Rule(rule_config1)
        rule2 = Rule(rule_config2)

        # When
        name_validator = NameValidator()

        # Then this should not raise an exception
        name_validator.validate(rule1, sample_df)
        with pytest.raises(ValueError, match="name is already in use"):
            # But this SHOULD raise an exception
            name_validator.validate(rule2, sample_df)

    def test_many_rules_correct(self, spark, sample_df, sample_rule_config):
        """
        Test validation of multiple rules with correct names.

        Validates that the NameValidator does not raise an exception for multiple rules
        with unique and valid names, although they are the same in terms of logic.
        """
        # Given
        rule_cfg2 = {
            RULE_NAME: "rule2",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_cfg3 = {
            RULE_NAME: "rule3",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_config1 = RuleConfig(**sample_rule_config)
        rule_config2 = RuleConfig(**rule_cfg2)
        rule_config3 = RuleConfig(**rule_cfg3)
        rule1 = Rule(rule_config1)
        rule2 = Rule(rule_config2)
        rule3 = Rule(rule_config3)

        # When
        name_validator = NameValidator()

        # Then this should not raise an exception
        name_validator.validate(rule1, sample_df)
        name_validator.validate(rule2, sample_df)
        name_validator.validate(rule3, sample_df)

    def test_single_rule_empty_output_col_name(self, spark, sample_df):
        """
        Test validation of a rule with an empty output column name.

        Validates that the NameValidator raises a ValueError for a rule
        with an empty output column name.
        """
        # Given
        rule_cfg = {
            RULE_NAME: "rule2",
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "", "operation": "value + 1"}],
        }
        rule_config = RuleConfig(**rule_cfg)
        rule = Rule(rule_config)

        # When
        name_validator = NameValidator()

        # Then this SHOULD raise an exception with a specific message
        with pytest.raises(ValueError, match="output column name cannot be empty"):
            name_validator.validate(rule, sample_df)

    def test_multiple_rule_duplicate_history_col_name(self, spark, sample_df):
        """
        Test validation of rules with duplicate history column names.

        Validates that the NameValidator raises a ValueError for rules
        that result in duplicate history column names.
        """
        # Given
        rule_cfg1 = {
            RULE_NAME: "ru_le",  # This will create a history column named "rule_new_column"
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "new_column", "operation": "value + 1"}],
        }
        rule_cfg2 = {
            RULE_NAME: "ru",  # This will also create a history column named "rule_new_column"
            CONDITIONS: [{"expression": "id == 1"}],
            ACTIONS: [{"output_col_name": "le_new_column", "operation": "value + 1"}],
        }
        rule_config1 = RuleConfig(**rule_cfg1)
        rule_config2 = RuleConfig(**rule_cfg2)
        rule1 = Rule(rule_config1)
        rule2 = Rule(rule_config2)

        # When
        name_validator = NameValidator()

        # Then this should not raise an exception
        name_validator.validate(rule1, sample_df)

        with pytest.raises(
            ValueError, match="column name is reserved for the history dataframe"
        ):
            # But this SHOULD raise an exception with a specific message
            name_validator.validate(rule2, sample_df)

"""
    Ensures that the names assigned to rules or columns are not repeated and do not conflict
    with the engine.
"""
from typing import List

from pyspark.sql.dataframe import DataFrame

from src.rule import Rule
from src.utils.validator.base_validator import BaseValidator


class NameValidator(BaseValidator):  # pylint: disable=too-few-public-methods
    """
    Ensures that the names assigned to rules or columns are not repeated and do not conflict
    with the engine.

    Names cannot be empty, nor can they be the same as the engine's reserved names, such as
    the ones used for the history dataframe.

    Attributes:
        current_rule_names (List[str]): List of the names of the defined rules.
        current_history_names (List[str]): List of the names of the history columns.
    """

    def __init__(self):
        """
        Initializes the NameValidator class.

        It uses two lists to keep track of the names of the defined rules and the history columns,
        avoiding repetitions.
        """
        super().__init__()
        self.current_rule_names: List[str] = []
        self.current_history_names: List[str] = []

    def validate(self, rule: Rule, df: DataFrame) -> None:
        """
        Validates the names of rules and their actions.
        Rules and actions must have unique names and cannot conflict with the history dataframe.

        Args:
            rule (Rule): The rule to be validated.
            df (DataFrame): The DataFrame to be used for validation.

        Raises:
            ValueError: If any name in rules or their actions is invalid.
        """
        # Validate the rule name
        if rule.rule_name == "":
            raise ValueError(
                f"Error on rule '{rule.rule_name}' definition: "
                "the rule name cannot be empty."
            )
        if rule.rule_name in self.current_rule_names:
            raise ValueError(
                f"Error on rule '{rule.rule_name}' definition: "
                "the rule name is already in use. Please choose a different name."
            )
        if rule.rule_name in df.columns:
            raise ValueError(
                f"Error on rule '{rule.rule_name}' definition: "
                "the rule name is the same as a column in the history DataFrame. Please choose a"
                "different name."
            )
        self.current_rule_names.append(rule.rule_name)

        # Validate each action, checking if the column name is already present in the
        # history dataframe
        for action in rule.actions:
            if action.output_col_name == "":
                raise ValueError(
                    f"Error on rule '{rule.rule_name}' definition: "
                    "the output column name cannot be empty."
                )
            name = action.get_history_column_name()
            if name in self.current_history_names or name in df.columns:
                raise ValueError(
                    f"Error on rule '{rule.rule_name}' definition: "
                    "the column name is reserved for the history dataframe. Please choose a"
                    "different name."
                )
            self.current_history_names.append(name)

"""
    Rule interface
"""
from functools import reduce
from typing import List

import pyspark
from pydantic import BaseModel, Field, field_validator

from src.action import Action, ActionConfig
from src.condition import Condition, ConditionConfig
from src.config.consts import ACTIONS, CONDITIONS, RULE_NAME
from src.utils.logger import logger


class RuleConfig(BaseModel):
    """
    Rule configuration class.

    Each rule is of the form:
    {
        "RULE_NAME": "rule_name",
        "CONDITIONS": [
            {
                ConditionConfig (can be empty)
            },
            ...
        ],
        "ACTIONS": [
            {
                ActionConfig
            },
            ...
        ]
    }
    """

    rule_name: str = Field(alias=RULE_NAME)
    conditions: List[ConditionConfig] = Field(alias=CONDITIONS, default=[])
    actions: List[ActionConfig] = Field(alias=ACTIONS)

    @field_validator(ACTIONS)
    @classmethod
    def check_non_empty_actions(cls, value):
        """
        Checks if the list of actions is not empty.

        Args:
            cls(RuleConfig): Rule configuration class.
            value(list[ActionConfig]): List of actions to be checked.

        Raises:
            ValueError: If the list of actions is empty.
        """
        if not value:  # If list is empty
            raise ValueError(f"{ACTIONS} must contain at least one action.")
        return value


class Rule:
    """
    Rule class.
    Each rule consists of a name, a list of conditions and a list of actions.

    Attributes:
        rule_name(str): Name of the rule.
        conditions(list[Condition]): List of conditions that must be met to apply the rule.
        actions(list[Action]): List of actions that will be applied if the conditions are met.
    """

    def __init__(
        self,
        rule_config: RuleConfig,
    ):
        """
        Initializes a Rule with conditions and actions.

        Args:
            rule_config(RuleConfig): Rule configuration. It's a dictionary
            with the rule name, a list of conditions and a list of actions.
        """
        self.rule_name = rule_config.rule_name

        if rule_config.conditions == []:
            # If no conditions are provided, it means that the rule will be applied to all the data,
            # so we add a default condition that will always be true.
            self.conditions = [Condition(ConditionConfig())]
        else:
            self.conditions = [
                Condition(condition) for condition in rule_config.conditions
            ]

        self.actions = [Action(action) for action in rule_config.actions]

        logger.debug(
            "Rule %s initialized with %d conditions and %d actions.",
            self.rule_name,
            len(self.conditions),
            len(self.actions),
        )

    def evaluate_rule_conditions(self) -> pyspark.sql.column.Column:
        """
        Calls every evaluate method of the conditions and returns the final set of conditions,
        from which the dataframe will be filtered.

        Returns:
            set_conditions(pyspark.sql.column.Column): Final conditions for filtering.
        """
        logger.debug("Evaluating conditions for rule: %s", self.rule_name)

        check_conditions = []

        for condition in self.conditions:
            check_conditions.append(condition.evaluate())

        set_conditions = reduce(lambda x, y: x & y, check_conditions)

        logger.debug("Conditions evaluated for rule: %s", self.rule_name)

        return set_conditions

    def apply(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Applies the rule to the dataframe, calling the execute method of each action.

        Args:
            df(pyspark.sql.DataFrame): data to which the rule will be applied.

        Returns:
            resultant_df(pyspark.sql.DataFrame): Resultant dataframe after applying the rule.
        """
        logger.debug("Applying rule: %s", self.rule_name)
        set_conditions = self.evaluate_rule_conditions()

        resultant_df = df.select("*")
        for action in self.actions:
            resultant_df = action.execute(resultant_df, set_conditions)

        logger.debug("Rule %s applied successfully.", self.rule_name)

        return resultant_df

    def __str__(self) -> str:
        """
        Example of the string representation of a rule:
        Rule rule_name
        Conditions
        Condition1, Condition2, ...
        Actions
        Action1, Action2, ...
        """
        return (
            f"Rule {self.rule_name}\n"
            f"Conditions\n{', '.join(str(condition) for condition in self.conditions)}\n"
            f"Actions\n{', '.join(str(action) for action in self.actions)}"
        )

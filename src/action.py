"""
    Action interface
"""
import pyspark
from typing import Set
from functools import reduce
from pydantic import BaseModel, Field
from pyspark.sql.functions import col, expr, when, lit

from src.config.consts import OPERATION, OTHERWISE, OUTPUT_COL_NAME
from src.enums.rule_type_enum import ExecutionType


class ActionConfig(BaseModel):
    """
    Action configuration class.

    Each action is of the form:
    {
        "OUTPUT_COL_NAME": "new_column_name",
        "OPERATION": "expression",
        "OTHERWISE": "expression"
    }
    """

    output_col_name: str = Field(alias=OUTPUT_COL_NAME)
    operation: str = Field(alias=OPERATION)
    otherwise: str = Field(alias=OTHERWISE, default=None)


class Action:
    """
    Actions specify what the system should do if the rule's conditions are met.
    They are defined by the output column name and the operation to be performed on
    that said column.

    Attributes:
        output_col_name (str): The name of the column to be created/modified.
        operation (str): The operation to be performed on the column.
        otherwise (str): The operation to be performed if the conditions are not met.
        rule_name (str): The name of the rule to which the action belongs.
        rule_type (ExecutionType): The type of the rule to which the action belongs.
    """

    def __init__(self, action_config: ActionConfig, rule_name: str, rule_type: ExecutionType):
        """
        Initializes the action.

        Args:
            action_config (ActionConfig): The definition of the action (one).
            It's a dictionary representing a JSON object.
            rule_name (str): The name of the rule to which the action belongs.
            rule_type (ExecutionType): The type of the rule to which the action belongs.
        """
        self.output_col_name = action_config.output_col_name
        self.operation = action_config.operation
        self.otherwise = action_config.otherwise
        self.rule_name = rule_name
        self.rule_type = rule_type

    def execute(
        self,
        df: pyspark.sql.DataFrame,
        set_conditions: pyspark.sql.column.Column,
        applied_rules_in_groups: Set[str],
    ) -> pyspark.sql.DataFrame:
        """
        Executes the action on the given DataFrame.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame to which the action will be applied.
            set_conditions (pyspark.sql.column.Column): Conditions that need to be met
            in order for the action to be applied.
            applied_rules_in_groups (Set[str]): Set of rules that have been applied in the cascade group.
        Returns:
            (pyspark.sql.DataFrame): Resultant DataFrame after executing the action.
            It also contains the history column respective to the action.
        """
        # Determine "otherwise" value
        if self.otherwise is not None:
            otherwise_value = expr(self.otherwise)
        elif self.output_col_name in df.columns: # If otherwise is None, maintain the previous value
            otherwise_value = col(self.output_col_name)
        else:
            otherwise_value = lit(None)

        # Determine if each row is affected or not by the cascade condition
        if self.rule_type == ExecutionType.CASCADE:
            cascade_blocked = reduce(
                lambda x, y: x | y, [col(rule_name) for rule_name in applied_rules_in_groups]
            )
            # Capture the previous values as a separate column before modifying df
            df = df.withColumn("_previous_values", col(self.output_col_name) if self.output_col_name in df.columns else lit(None))

        # Apply the action
        df = df.withColumn(
            self.output_col_name,
            when(set_conditions, expr(self.operation)).otherwise(otherwise_value),
        )

        # Restore the previous values if the action was blocked by the cascade condition
        if self.rule_type == ExecutionType.CASCADE:
            df = df.withColumn(
                self.output_col_name,
                when(cascade_blocked, col("_previous_values")).otherwise(col(self.output_col_name)),
            ).drop("_previous_values")  # Cleanup temporary column

        # Add action history column
        return df.withColumn(
            self.get_history_column_name(), df[self.output_col_name]
        )

    def get_history_column_name(self) -> str:
        """
        Returns the name of the history column for this action.
        It's of the form "rule_name_output_col_name". For example, if the rule name is
        "rule1" and the output column name is "new_column", the history column name will
        be "rule1_new_column".

        Returns:
            (str): The name of the history column.
        """
        return f"{self.rule_name}_{self.output_col_name}"

    def __str__(self) -> str:
        """
        Returns a string representation of the action.

        Returns:
            (str): the string representation of the action.
        """
        return f"{self.output_col_name} = {self.operation}"

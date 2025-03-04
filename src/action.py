"""
    Action interface
"""
import pyspark
from pydantic import BaseModel, Field
from pyspark.sql.functions import col, expr, when

from src.config.consts import OPERATION, OTHERWISE, OUTPUT_COL_NAME


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
    """

    def __init__(self, action_config: ActionConfig, rule_name: str):
        """
        Initializes the action.

        Args:
            action_config (ActionConfig): The definition of the action (one).
            It's a dictionary representing a JSON object.
        """
        self.output_col_name = action_config.output_col_name
        self.operation = action_config.operation
        self.otherwise = action_config.otherwise
        self.rule_name = rule_name

    def execute(
        self,
        df: pyspark.sql.DataFrame,
        set_conditions: pyspark.sql.column.Column,
    ) -> pyspark.sql.DataFrame:
        """
        Executes the action on the given DataFrame.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame to which the action will be applied.
            set_conditions (pyspark.sql.column.Column): Conditions that need to be met
            in order for the action to be applied.
        Returns:
            (pyspark.sql.DataFrame): Resultant DataFrame after executing the action.
            It also contains the history column respective to the action.
        """
        if self.output_col_name in df.columns:
            otherwise_value = col(self.output_col_name)
        elif self.otherwise is not None:
            otherwise_value = expr(self.otherwise)
        else:
            otherwise_value = None

        df = df.withColumn(
            self.output_col_name,
            when(set_conditions, expr(self.operation)).otherwise(otherwise_value),
        )

        return df.withColumn(  # Add action history column
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

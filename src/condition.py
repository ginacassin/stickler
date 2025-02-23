"""
    Condition interface
"""
import pyspark
from pyspark.sql.functions import expr
from pydantic import BaseModel, Field

from src.config.consts import EXPRESSION


class ConditionConfig(BaseModel):
    """
    Condition configuration class.

    Each condition is of the form:
    {
        "EXPRESSION": "expression"
    }

    If no configuration is provided, the condition is applied to all data,
    so the expression is set to "true".
    """

    expression: str = Field(alias=EXPRESSION, default="true")


class Condition:
    """
    Conditions are used to filter data based on a given expression,
    they outline the situation in which an action should occur.

    Attributes:
        expression (str): The expression to be evaluated on the data.
    """

    def __init__(self, condition_config: ConditionConfig):
        """
        Initializes the condition with the given expression.

        Args:
            condition_config (ConditionConfig): The definition of the condition
            expression (one). It's a dictionary representing a JSON object.
        """
        self.expression = condition_config.expression

    def evaluate(self) -> pyspark.sql.Column:
        """
        Evaluates the condition on the data.

        Returns:
            (pyspark.sql.Column): Condition in column form (in order to apply
            'when' logic during the execution of actions).
        """
        return expr(self.expression).alias("condition")

    def __str__(self) -> str:
        """
        Returns a string representation of the condition.

        Returns:
            (str): The string representation of the condition.
        """
        return self.expression

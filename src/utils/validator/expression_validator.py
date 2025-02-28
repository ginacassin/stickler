"""
    Ensures that expressions in conditions and actions are valid Spark SQL expressions.
"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException

from src.rule import Rule
from src.utils.validator.base_validator import BaseValidator


class ExpressionValidator(BaseValidator): # pylint: disable=too-few-public-methods
    """
    Ensures that expressions in conditions and actions are valid Spark SQL expressions.

    An invalid expression could be "age >> 30 AND income << 50000", which uses incorrect
    operators.

    Attributes:
        validation_df (DataFrame): An auxiliary DataFrame used to validate the expressions.
    """

    def __init__(self):
        """
        Initializes the ExpressionValidator class.

        It uses an auxiliary DataFrame to validate the expressions in the conditions and actions
        of a rule.
        """
        super().__init__()
        self.validation_df = None

    def validate(self, rule: Rule, df: DataFrame) -> None:
        """
        Validates the expressions in the conditions and actions of a rule.

        Args:
            rule (Rule): The rule to be validated.
            df (DataFrame): The DataFrame to be used for validation.
        
        Raises:
            ValueError: If any expression in the conditions or actions is invalid.
        """
        # Initialize the validation DataFrame if it hasn't been already
        if not self.validation_df:
            self.validation_df = df.select("*").limit(0)

        # Validate each condition's expression in the rule
        for condition in rule.conditions:
            expression = condition.expression
            try:
                self.validation_df.select(expr(expression))
            except AnalysisException as exc:
                raise ValueError(
                    f"Error on rule '{rule.rule_name}' definition: "
                    f"the expression '{expression}' is invalid. Please use a valid "
                    f"operator and syntax."
                ) from exc

        # Validate each action's expression in the rule
        for action in rule.actions:
            try:
                self.validation_df = self.validation_df.withColumn(
                    action.output_col_name, expr(action.operation)
                )
            except AnalysisException as exc:
                raise ValueError(
                    f"Error on rule '{rule.rule_name}' definition: "
                    f"the expression '{action.operation}' is invalid. Please use a valid "
                    f"operator and syntax."
                ) from exc

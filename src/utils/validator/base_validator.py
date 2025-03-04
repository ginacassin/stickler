"""
    Abstract base class for rule validation
"""
from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame

from src.rule import Rule


class BaseValidator(ABC):  # pylint: disable=too-few-public-methods
    """
    Abstract base class for rule validation.
    Each validator checks a specific aspect of the rule definition.
    """

    @abstractmethod
    def validate(self, rule: Rule, df: DataFrame) -> None:
        """
        Validate a single rule against the DataFrame and a shared context.

        Args:
            rule (Rule): The rule to validate.
            df (DataFrame): The DataFrame to validate the rule against.
        """

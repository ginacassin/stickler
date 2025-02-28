"""
    Chains multiple validators to run sequentially on each rule.
"""
from typing import List

from pyspark.sql.dataframe import DataFrame

from src.rule import Rule
from src.utils.validator.base_validator import BaseValidator


class ValidatorChain:
    """
    Chains multiple validators to run sequentially on each rule.

    Attributes:
        validators (List[BaseValidator]): List of validators to run on each rule.
    """

    def __init__(self):
        """
        Initializes the validator chain with an empty list of validators.
        """
        self.validators: List[BaseValidator] = []

    def add_validator(self, validator: BaseValidator):
        """
        Adds a new validator to the chain.

        Args:
            validator (BaseValidator): A validator to add to the chain.
        """
        self.validators.append(validator)

    def validate(self, rules: List[Rule], df: DataFrame) -> None:
        """
        Initialize the context with the DataFrame's original columns.
        
        Args:
            rules (List[Rule]): List of rules to validate.
            df (DataFrame): DataFrame to validate against.
        """
        for rule in rules:
            for validator in self.validators:
                validator.validate(rule, df)

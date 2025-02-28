"""
    RuleEngine
"""
from typing import List

import pyspark
from pydantic import BaseModel, Field

from src.config.consts import RULES_STARTER
from src.rule import Rule, RuleConfig
from src.utils.logger import logger
from src.utils.validator.expression_validator import ExpressionValidator
from src.utils.validator.reference_validator import ReferenceValidator
from src.utils.validator.validator_chain import ValidatorChain


class RulesConfig(BaseModel):
    """
    Rules configuration class to load rules from a JSON file.

    The structure of the rules is:
    {
        "RULES_STARTER": [
            {
               RuleConfig
            },
            {
               RuleConfig
            },
            ...
        ]
    }
    """

    rules: List[RuleConfig] = Field(alias=RULES_STARTER)


class RuleEngine:
    # pylint: disable=too-few-public-methods
    """
    RuleEngine class is responsible for applying rules to the given DataFrame.

    Attributes:
        rules (List[Rule]): List of Rules, loaded from the configuration.
    """

    def __init__(self, rules_config: RulesConfig):
        """
        Initializes the rule engine with rule configurations.

        Args:
            rules_config (RulesConfig): Dictionary containing rule configurations,
            which is read with json.load() from a JSON file.
        """
        logger.info("Initializing RuleEngine with provided rule configurations.")
        self.rules = [Rule(rule_data) for rule_data in rules_config.rules]
        logger.debug("Loaded %d rules from configuration.", len(self.rules))

        # Initialize the validator chain with the required validators.
        self.validator_chain = ValidatorChain()
        # Reference errors are less broad than general expression errors,
        # so they should be checked first.
        self.validator_chain.add_validator(ReferenceValidator())
        self.validator_chain.add_validator(ExpressionValidator())
        logger.info("Validators initialized and added to the validator chain.")

    def apply_rules(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Applies all rules to the given DataFrame.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame to which rules will be applied.

        Returns:
            df (pyspark.sql.DataFrame): Resultant DataFrame after applying all rules.
        """
        self.validator_chain.validate(self.rules, df)

        for rule in self.rules:
            df = rule.apply(df)
            logger.info("%s", str(rule))

        logger.info("All rules applied successfully.")

        return df

"""
    RuleEngine
"""
from typing import List, Tuple

import pyspark
from pydantic import BaseModel, Field

from src.config.consts import RULES_STARTER
from src.rule import Rule, RuleConfig
from src.utils.logger import logger
from src.utils.validator.expression_validator import ExpressionValidator
from src.utils.validator.name_validator import NameValidator
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
        self.validator_chain.add_validator(NameValidator())
        # Reference errors are less broad than general expression errors,
        # so they should be checked first.
        self.validator_chain.add_validator(ReferenceValidator())
        self.validator_chain.add_validator(ExpressionValidator())
        logger.info("Validators initialized and added to the validator chain.")

    def apply_rules(
        self, df: pyspark.sql.DataFrame
    ) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        """
        Applies all rules to the given DataFrame, previously validating the input.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame to which rules will be applied.

        Returns:
            output_df (pyspark.sql.DataFrame): Resultant DataFrame after applying all rules.
            history_df (pyspark.sql.DataFrame): DataFrame indicating if a rule was applied or not,
            and the value after executing each of its actions.
            History columns are noted with the rule name as a prefix.
        """
        self.validator_chain.validate(self.rules, df)

        original_columns = df.columns
        history_columns = []

        for rule in self.rules:
            df = rule.apply(df)
            logger.info("%s", str(rule))

            # Get names of all the history columns in the DataFrame
            history_columns.append(rule.rule_name)
            for action in rule.actions:
                history_columns.append(action.get_history_column_name())

        # List columns for output_df (excluding history columns)
        computed_columns = [
            col
            for col in df.columns
            if col not in original_columns and col not in history_columns
        ]

        # Final output DataFrame: original columns + computed columns
        output_df = df.select(*(original_columns + computed_columns))

        # History DataFrame: original columns + history columns
        history_df = df.select(*(original_columns + history_columns))

        logger.info("All rules applied successfully.")

        return output_df, history_df

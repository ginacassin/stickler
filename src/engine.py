"""
    RuleEngine
"""
from typing import Dict, List, Tuple

import pyspark
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

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
        udfs (Dict[str, pyspark.sql.functions.udf]): Dictionary of user-defined functions (UDFs).
        validator_chain (ValidatorChain): Chain of validators to validate rules and DataFrame.
    """

    def __init__(
        self,
        rules_config: RulesConfig,
        udfs: Dict[str, pyspark.sql.functions.udf] = None,
    ):
        """
        Initializes the rule engine with rule configurations.

        Args:
            rules_config (RulesConfig): Dictionary containing rule configurations,
            which is read with json.load() from a JSON file.
            udfs (Dict[str, pyspark.sql.functions.udf], optional): Dictionary of
            user-defined functions (UDFs) to be used in rules.
        """
        logger.info("Initializing RuleEngine with provided rule configurations.")
        self.rules = [Rule(rule_data) for rule_data in rules_config.rules]
        logger.debug("Loaded %d rules from configuration.", len(self.rules))
        self.udfs = udfs if udfs else {}

        spark = SparkSession.getActiveSession()
        # Register UDFs in the Spark session
        for udf_name, udf_func in self.udfs.items():
            if not hasattr(udf_func, "func"):  # Validate if the function is a UDF
                raise ValueError(f"UDF {udf_name} is not a valid UDF function.")
            spark.udf.register(udf_name, udf_func)
            logger.debug('UDF "%s" registered successfully.', udf_name)
        logger.debug("%d user-defined functions (UDFs) loaded.", len(self.udfs))

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

        # Dictionary to keep track of applied rules in groups
        groups: Dict[int, List[str]] = {}

        for rule in self.rules:
            # Get names of all rules belonging to the same group as the current rule
            applied_rules_in_groups = set()
            for group in rule.cascade_group:
                applied_rules_in_groups.update(set(groups.get(group, [])))

            df = rule.apply(df, applied_rules_in_groups)
            logger.info("%s", str(rule))

            # Add rule to the executed list
            for group in rule.cascade_group:
                groups.setdefault(group, []).append(rule.rule_name)

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

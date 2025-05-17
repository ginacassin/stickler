"""
    Validates that any column referenced in a rule's conditions or actions exists either in the
    original DataFrame schema or has been defined in a prior rule.
"""
import re
from typing import List, Set

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.rule import Rule
from src.utils.validator.base_validator import BaseValidator


class ReferenceValidator(BaseValidator):
    """
    Validates that any column referenced in a rule's conditions or actions exists either in the
    original DataFrame schema or has been defined in a prior rule.

    This validator uses a simple regex to extract potential column references from an expression
    string. It ignores any tokens that are SQL keywords or function names, as well as any tokens
    that are purely numeric.

    For example, given the expression "col1 + col2", this validator would extract "col1" and "col2"
    as potential column references.

    Note that this validator does not validate the types of the columns being referenced, only their
    existence. It is up to the user to ensure that the types of the columns are compatible with the
    operations being performed on them.

    Attributes:
        ignore_words (Set[str]): A set of words to ignore when extracting potential
        column references.
        df_current_column_names(Set): A set of column names that have been defined
        in the DataFrame so far.
    """

    def __init__(self):
        """
        Initialize the ReferenceValidator.

        The ReferenceValidator uses a SparkSession to retrieve a list of SQL functions to ignore
        when extracting potential column references from an expression string. It also adds "true"
        and "false" to the list of words to ignore, as these are not valid column names.

        It also initializes a set of column names that have been defined in the DataFrame so far.
        This set is updated as new columns are defined in the DataFrame by the rules.
        """
        super().__init__()
        # Words to ignore (functions, operators, etc.)
        spark = SparkSession.getActiveSession()
        functions_df = spark.sql("SHOW FUNCTIONS")
        self.ignore_words: Set[str] = {row.function for row in functions_df.collect()}
        self.ignore_words.add("true")
        self.ignore_words.add("false")
        self.df_current_column_names: Set = None

    def extract_columns(self, expression: str) -> List[str]:
        """
        Extract potential column references from an expression string.

        Args:
            expression (str): The expression string to extract column references from.

        Returns:
            List[str]: A list of unique potential column references extracted from the
            expression string.
        """
        identifier_regex = r"[a-zA-Z_][a-zA-Z0-9_]*"

        # Get spans of quoted regions (e.g. 'a', "a")
        quoted_spans = [m.span() for m in re.finditer(r"(['\"]).*?\1", expression)]

        tokens = []
        # Use re.finditer so we get span positions for each token.
        for token_match in re.finditer(identifier_regex, expression):
            token = token_match.group(0)
            token_span = token_match.span()

            # Check if the token is within any quoted region.
            in_quote = any(
                start <= token_span[0] and token_span[1] <= end
                for (start, end) in quoted_spans
            )
            if not in_quote:
                tokens.append(token)

        # Filter tokens for rules: not an ignore word, not a digit, not an operator (like AND, OR)
        unique_columns = []
        for token in tokens:
            if (
                token.lower() not in self.ignore_words
                and not token.isdigit()
                and token not in unique_columns  # ensure uniqueness
            ):
                unique_columns.append(token)

        return unique_columns

    def validate(self, rule: Rule, df: DataFrame) -> None:
        """
        Validates that any column referenced in a rule's conditions or actions exists either in the
        original DataFrame schema or has been defined in a prior rule.

        Args:
            rule (Rule): The rule to validate.
            df (DataFrame): The DataFrame to validate the rule against.

        Raises:
            ValueError: If a column referenced in a rule's conditions or actions does not exist
            in the original DataFrame schema or has not been defined in a prior rule.
        """
        # If this is the first rule, initialize the set of column names
        if self.df_current_column_names is None:
            self.df_current_column_names = set(df.columns)

        # Validate each condition in the rule
        for condition in rule.conditions:
            expression = condition.expression
            referenced = self.extract_columns(expression)
            for col in referenced:
                if col not in self.df_current_column_names:
                    raise ValueError(
                        f"Error on rule '{rule.rule_name}' definition: "
                        f"Column '{col}' doesn't exist."
                    )

        # Validate each action in the rule
        for action in rule.actions:
            operation = action.operation
            referenced = self.extract_columns(operation)
            for col in referenced:
                # Allow the column if it's the one being defined in this action
                if (
                    col not in self.df_current_column_names
                    and col != action.output_col_name
                ):
                    raise ValueError(
                        f"Error on rule '{rule.rule_name}' definition: "
                        f"Column '{col}' doesn't exist."
                    )

            # After validating the action, add new output column to the context
            self.df_current_column_names.add(action.output_col_name)

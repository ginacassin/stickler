"""
    Tests for the RuleEngine class.
"""

import json

from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.utils import assertDataFrameEqual

from src.engine import RuleEngine, RulesConfig
from src.rule import RuleConfig


class TestEngine:
    """
    This class contains tests to validate the behavior of the RuleEngine class.
    """

    def test_udfs_registration(self, spark, sample_df):
        """
        Test the registration of user-defined functions (UDFs).

        Validates that UDFs are correctly registered in the Spark session and
        can be applied to a sample DataFrame.
        """
        # Given sample UDFs
        def sample_udf_1(value):
            return value.upper()

        def sample_udf_2(value):
            return len(value)

        udfs = {
            "to_upper": udf(sample_udf_1, StringType()),
            "string_length": udf(sample_udf_2, IntegerType()),
        }

        rules_config = RulesConfig(rules=[])

        # When
        engine = RuleEngine(rules_config, udfs)

        # Check if UDFs are registered
        registered_function_names = [f.name for f in spark.catalog.listFunctions()]
        assert "to_upper" in registered_function_names
        assert "string_length" in registered_function_names

        # Apply UDFs to sample DataFrame
        df = sample_df.withColumn("upper_value", udfs["to_upper"](col("value")))
        df = df.withColumn("value_length", udfs["string_length"](col("value")))

        # Then
        expected_data = [
            (1, "a", "A", 1),
            (2, "b", "B", 1),
            (3, "c", "C", 1),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("upper_value", StringType(), True),
                StructField("value_length", IntegerType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        assertDataFrameEqual(df, expected_df)

    def test_apply_rules(self, spark, sample_df):
        """
        Test the application of rules to a DataFrame.

        Validates that rules are correctly applied to the input DataFrame and
        that the output and history DataFrames match the expected results.
        """
        # Given
        rule_config = RuleConfig(
            rule_name="test_rule",
            conditions=[{"expression": "id != 1"}],
            actions=[{"output_col_name": "new_col", "operation": "100"}],
        )
        rules_config = RulesConfig(rules=[rule_config])

        # When
        engine = RuleEngine(rules_config)

        # Apply rules to sample DataFrame
        output_df, history_df = engine.apply_rules(sample_df)

        # Then
        # Check output DataFrame
        expected_output_data = [
            (1, "a", None),
            (2, "b", 100),
            (3, "c", 100),
        ]
        expected_output_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("new_col", IntegerType(), True),
            ]
        )
        expected_output_df = spark.createDataFrame(
            expected_output_data, expected_output_schema
        )

        assertDataFrameEqual(output_df, expected_output_df)

        # Check the history DataFrame
        expected_history_data = [
            (1, "a", False, None),
            (2, "b", True, 100),
            (3, "c", True, 100),
        ]
        expected_history_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
                StructField("test_rule", BooleanType(), True),
                StructField("test_rule_new_col", IntegerType(), True),
            ]
        )
        expected_history_df = spark.createDataFrame(
            expected_history_data, expected_history_schema
        )

        assertDataFrameEqual(history_df, expected_history_df)

    def test_discount_cascade_logic(self, spark):
        """
        Test the cascade logic for discount rules.

        Validates that rules in different cascade groups are applied in the
        correct order and that blocked rules are handled appropriately.
        """
        # Given
        data = [
            # 1) Eligible for repeat (group 0) & seasonal (group 1) discounts: skip newsletter
            {
                "customer_id": 1,
                "price": 120.0,
                "previous_orders": 2,
                "season": "spring",
                "is_subscriber": True,
            },
            # 2) Not repeat, not seasonal: eligible for newsletter (group 0&1)
            {
                "customer_id": 2,
                "price": 200.0,
                "previous_orders": 0,
                "season": "summer",
                "is_subscriber": True,
            },
            # 3) Eligible for repeat only: skip newsletter
            {
                "customer_id": 3,
                "price": 80.0,
                "previous_orders": 3,
                "season": "winter",
                "is_subscriber": False,
            },
            # 4) Eligible for seasonal only: skip newsletter
            {
                "customer_id": 4,
                "price": 150.0,
                "previous_orders": 0,
                "season": "spring",
                "is_subscriber": False,
            },
        ]

        df_input = spark.createDataFrame(data)

        rules_json = """
        {
        "rules": [
            {
                "rule_name": "RepeatCustomerDiscount",
                "execution_type": "accumulative",
                "cascade_group": 0,
                "conditions": [
                    {
                    "expression": "previous_orders > 0"
                    }
                ],
                "actions": [
                    {
                    "output_col_name": "price",
                    "operation": "price * 0.90"
                    }
                ]
            },
            {
                "rule_name": "SeasonalDiscount",
                "cascade_group": 1,
                "conditions": [
                    {
                    "expression": "season == 'spring'"
                    }
                ],
                "actions": [
                    {
                    "output_col_name": "price",
                    "operation": "price * 0.95"
                    }
                ]
            },
            {
                "rule_name": "NewsletterDiscount",
                "execution_type": "cascade",
                "cascade_group": [0, 1],
                "conditions": [
                    {
                    "expression": "is_subscriber == true"
                    }
                ],
                "actions": [
                    {
                    "output_col_name": "price",
                    "operation": "price * 0.85"
                    }
                ]
            }
        ]
        }
        """
        rules_json = json.loads(rules_json)
        rules_config = RulesConfig(**rules_json)

        expected_output = [
            (
                1,
                120.0 * 0.95 * 0.90,
            ),  # spring & repeat: Seasonal runs first (group1) then Repeat (group0). Newsletter blocked.
            (2, 200.0 * 0.85),  # only newsletter
            (3, 80.0 * 0.90),  # only repeat
            (4, 150.0 * 0.95),  # only seasonal
        ]
        expected_output_schema = StructType(
            [
                StructField("customer_id", LongType(), True),
                StructField("price", DoubleType(), True),
            ]
        )
        expected_output_df = spark.createDataFrame(
            expected_output, expected_output_schema
        )

        # Then
        engine = RuleEngine(rules_config)
        output_df, history_df = engine.apply_rules(df_input)

        assertDataFrameEqual(
            output_df.select("customer_id", "price"), expected_output_df
        )

        expected_history = [
            # id,   original_price, Repeat applied?, Seasonal applied?, Newsletter applied?, rule_price
            (
                1,
                120.0 * 0.95 * 0.90,
                True,
                True,
                False,
                120.0 * 0.95 * 0.90,
            ),
            (2, 200.0 * 0.85, False, False, True, 200.0 * 0.85),
            (3, 80.0 * 0.90, True, False, False, 80.0 * 0.90),
            (4, 150.0 * 0.95, False, True, False, 150.0 * 0.95),
        ]
        expected_history_schema = StructType(
            [
                StructField("customer_id", LongType(), True),
                StructField("price", DoubleType(), True),
                StructField("RepeatCustomerDiscount", BooleanType(), False),
                StructField("SeasonalDiscount", BooleanType(), False),
                StructField("NewsletterDiscount", BooleanType(), False),
                StructField("NewsletterDiscount_price", DoubleType(), True),
            ]
        )
        expected_history_df = spark.createDataFrame(
            expected_history, expected_history_schema
        )

        assertDataFrameEqual(
            history_df.select(
                "customer_id",
                "price",
                "RepeatCustomerDiscount",
                "SeasonalDiscount",
                "NewsletterDiscount",
                "NewsletterDiscount_price",
            ),
            expected_history_df,
        )

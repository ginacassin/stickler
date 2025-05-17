"""
Constants used for parsing the input file

The input file is a text file with the following format:
{
    "rules": [
        {
            "rule_name": "rule_name1",
            "execution_type": "cascade"/"accumulative"
            "cascade_group": [0, 1, 2] / 0,
            "conditions": [
                {
                    "expression": "sql_expression"
                }, …
            ],
            "actions": [
                {
                    "output_col_name": "column_name",
                    "operation": "sql_expression"
                }, …
            ]
        }, …
    ]
}
"""

RULES_STARTER = "rules"
RULE_NAME = "rule_name"
EXECUTION_TYPE = "execution_type"
CASCADE_GROUP = "cascade_group"
CONDITIONS = "conditions"
EXPRESSION = "expression"
ACTIONS = "actions"
OUTPUT_COL_NAME = "output_col_name"
OPERATION = "operation"
OTHERWISE = "otherwise"

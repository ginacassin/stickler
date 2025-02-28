"""
Constants used for parsing the input file

The input file is a text file with the following format:
{
    "rules": [
        {
            "rule_name": "rule_name1",
            "conditions": [
                {
                    "expression": "expression"
                }, …
            ],
            "actions": [
                {
                    "output_col_name": "output",
                    "operation": "operation_def"
                }, …
            ]
        }, …
    ]
}
"""

RULES_STARTER = "rules"
RULE_NAME = "rule_name"
CONDITIONS = "conditions"
EXPRESSION = "expression"
ACTIONS = "actions"
OUTPUT_COL_NAME = "output_col_name"
OPERATION = "operation"
OTHERWISE = "otherwise"

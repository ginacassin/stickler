# Stickler Business Rule Engine
True to its name, Stickler is all about following the rules—your business rules, that is! 

Stickler is a scalable, high-performance business rule engine built on PySpark for efficient rule evaluation and integration with big data pipelines.


---
## 🚀 Features

* **Scalability**: Handles large datasets efficiently using Spark's distributed computing capabilities.
* **PySpark native**: Built specifically for PySpark.
* **Universal applicability**: Designed to integrate into various project types and scopes.
* **Execution control**: Supports different rule execution types to align with business requirements.
* **History tracking**: Records all data alterations during rule execution for transparency and auditability.
* **User-friendliness**: Allows rule definition via simple JSON expressions, accessible to users with varying technical expertise.

![](/.github/diagram.jpg)

Stickler takes in an **input PySpark DataFrame**, a **JSON file** defining rules, and optional User-Defined Functions (**UDFs**); the engine then **validates** these inputs and executes the rules, subsequently producing **two output PySpark DataFrames**: one with the transformed data and another detailing the execution history.

---
## 📦 Installation

```bash
pip install stickler
```

---
## ⚙️ Usage

### Importing Stickler

Once installed, import the engine in your Python script: 

```python
from stickler import engine
```

### Initializing the Engine
Rules are defined in a JSON file. Load this configuration and initialize the RuleEngine:

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("SticklerApp").getOrCreate()

# Load rules from a JSON file
with open("rules.json") as f:
    rules_data = json.load(f)

# Parse JSON into RulesConfig object
rules_configuration = engine.RulesConfig(**rules_data)
# Instantiate the engine
rule_engine_instance = engine.RuleEngine(rules_configuration) 
```

### Using User-Defined Functions (UDFs)

Stickler supports PySpark UDFs in rule expressions. Define your UDFs and pass them to the engine as a dictionary:

```python
from pyspark.sql.functions import udf

# Example UDF definitions
def custom_upper(value: str) -> str:
    return value.upper()

def get_string_length(value: str) -> int:
    return len(value)

# Register UDFs with Spark
udf_to_upper = udf(custom_upper, StringType())
udf_string_length = udf(get_string_length, IntegerType())

udfs_dictionary = {
    "to_upper": udf_to_upper,
    "string_length": udf_string_length
}

# Initialize engine with UDFs
rule_engine_instance_with_udfs = engine.RuleEngine(rules_configuration, udfs=udfs_dictionary)
```

### Executing the Engine (apply rules)

Assuming ```input_df``` is the input PySpark DataFrame:

```python
output_df, history_df = rule_engine_instance.apply_rules(input_df)
```

### Example
A very simple example is provided in [`main.py`](./main.py), which
uses [`input.json`](./input.json).

## 📜 JSON Decision Model (JDM) for Rule Definition

### Overview
Stickler uses a JSON-based format for defining business rules. This allows for easy configuration and management of rules in a structured, human-readable way. The JDM is a single JSON object containing a primary key, "rules", which holds an array of rule definition objects.

Rules are evaluated **sequentially** in the order they were defined, from **top to bottom**.

### Structure
Each rule object within the "rules" array has the following structure:

```json
{
    "rules": [
        {
            "rule_name": "rule_name1",
            "execution_type": "cascade"/"accumulative",
            "cascade_group": [0, 1, 2] / 0 (optional),
            "conditions": [
                {
                    "expression": "spark_sql_expression"
                }
            ],
            "actions": [
                {
                    "output_col_name": "column_name",
                    "operation": "spark_sql_expression",
                    "otherwise": "spark_sql_expression" (optional)
                }
            ]
        }, 
        ...
    ]
}
```

* **`rule_name`** (string): A unique and descriptive name for the rule.
* **`execution_type`** (string): Determines how the rule interacts with others.
    * `"accumulative"`: The rule is applied to every row satisfying its conditions, independently of other accumulative rules. Default if no type is defined.
    * `"cascade"`: Utilizes cascading groups. The rule executes on a row only if no other rule within the same `cascade_group` has already been applied to that row. Used to enforce precedence or mutual exclusivity.
* **`cascade_group`** (integer or array of integers, optional): Assigns the rule to one or more cascading groups.  Identified by integer values. Defaults to `0` if not specified for a cascade rule, or accumulative. If an accumulative rule is associated with a cascade group, its execution blocks later cascade rules in the same group(s) for that data row.
* **`conditions`** (array of objects): An array of condition objects. All conditions must evaluate to true (logical AND) for the rule's actions to be triggered.
    * `expression` (string): A Spark SQL boolean expression, e.g., `"original_price > 100"`. UDFs can be used here.
* **`actions`** (array of objects): Defines operations to perform if all conditions are met. Actions are limited to creating new columns or modifying existing ones.
    * `output_col_name` (string): The name of the column to be created or updated.
    * `operation` (string): A Spark SQL expression whose result is assigned to `output_col_name` if conditions are met, for example, `"original_price * 0.90"`. UDFs can be used here.
    * `otherwise` (string, optional): A Spark SQL expression defining an alternative operation if the rule's main conditions are not met. Its result is assigned to `output_col_name`. If main conditions are met, this is ignored. UDFs can be used here.

### Rule Execution Types
Each rule can be executed in two different types: accumulative or cascade.

* **Accumulative**: This rule type applies to every row that meets its conditions. Its changes are made independently and can affect later rules.
* **Cascade**: This rule type uses `cascading groups` for a more controlled flow. If a rule is in a cascade group and set to `"cascade"`, it runs on a row only if no other rule in the same group has already run on that row. 

---
## ✅ Rule Validation
Before running any rules, Stickler checks the rule definitions from the JSON file to catch errors early. This validation process checks for:

* **Syntax errors in JSON**: Ensures rules have the right fields and a valid JSON structure. 
* **Name validation**: Rule names must be filled in, unique, and not clash with names Stickler uses internally (such as those used in the history DataFrame, usually named `RuleName_OutputColumnName`). It flags issues like empty rule names or repeated rule names. 
* **Reference validation**: Makes sure any columns mentioned in conditions or actions already exist in the input data or were created by an earlier rule. If a column doesn't exist, it raises an error. 
* **Expression validation**: Catches common mistakes like wrong operators or syntax errors in the expressions.

---
## 📊 History Tracking

The history DataFrame includes:
* The original input columns.
* A yes/no (boolean) column for each rule (named after the rule), showing if it was applied to that row. 
* An extra column for each action that changed or created a column. This column follows the format `RuleName_OutputColumnName`, and shows the value that rule's action produced for that output column at that moment of execution. For example, 
`discountPrices_Price` would be the history column name for the rule `discountPrices` with output
column `Price`.

If a rule's conditions are met but it's blocked due to cascade execution (another rule in the same group was already applied), it will be marked as not executed (False) in its history column.
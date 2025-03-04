"""
Main example script to demonstrate the usage of stickler engine.
"""
import json
import logging

from pyspark.sql import SparkSession

from src.engine import RuleEngine, RulesConfig

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("stickler_example")

# Initialize Spark session
logger.info("Initializing Spark session")
spark_logger = logging.getLogger("py4j")
spark_logger.setLevel(logging.WARN)
spark = SparkSession.builder.appName("RuleEngine").getOrCreate()

# Sample dataframe
data = [(100, 0.2, 50, "John Doe"), (200, 0.4, 75, "JANE DOE")]
columns = ["productCost", "margin", "transaction_amount", "customer_name"]
df = spark.createDataFrame(data, columns)

df.show()
# Load rules JSON
with open("input.json") as f:
    rules_json = json.load(f)

# Run rule engine
rules_config = RulesConfig(**rules_json)
engine = RuleEngine(rules_config)
df_result, df_history = engine.apply_rules(df)

df_result.show()
df_history.show()
spark.stop()

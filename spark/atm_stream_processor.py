from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark Session with Iceberg Config
spark = SparkSession.builder \
    .appName("ATM Transaction Stream Processor") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "file:///tmp/iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("atm_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("transaction_time", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "atm-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ✅ CREATE DATABASE + TABLE (IMPORTANT)
spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.db")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.db.atm_transactions (
    transaction_id STRING,
    atm_id STRING,
    card_id STRING,
    amount INT,
    transaction_time STRING
) USING iceberg
""")

# ✅ WRITE STREAM TO ICEBERG
query = json_df.writeStream \
    .format("iceberg") \
    .option("path", "my_catalog.db.atm_transactions") \
    .option("checkpointLocation", "/tmp/checkpoints/atm_stream") \
    .outputMode("append") \
    .start()

query.awaitTermination()
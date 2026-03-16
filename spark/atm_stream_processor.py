from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("ATM Transaction Stream Processor") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("atm_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("transaction_time", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "atm-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/atm_db") \
        .option("dbtable", "atm_transactions") \
        .option("user", "atm_user") \
        .option("password", "atm_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
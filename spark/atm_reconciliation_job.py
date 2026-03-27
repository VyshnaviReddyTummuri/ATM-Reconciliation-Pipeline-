from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, expr
from pyspark.sql.functions import trim
from pyspark.sql import Row

# Create Spark Session
spark = SparkSession.builder \
    .appName("ATM Reconciliation Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read ATM data (from Iceberg parquet location)
atm_data = [
    Row(transaction_id="TXN1001", atm_id="ATM001", card_id="CARD1001", atm_amount=200, transaction_time="2026-03-13T10:00:00"),
    Row(transaction_id="TXN1002", atm_id="ATM002", card_id="CARD1002", atm_amount=500, transaction_time="2026-03-13T11:00:00"),
    Row(transaction_id="TXN1003", atm_id="ATM003", card_id="CARD1003", atm_amount=700, transaction_time="2026-03-13T12:00:00"),
    Row(transaction_id="TXN1004", atm_id="ATM004", card_id="CARD1004", atm_amount=300, transaction_time="2026-03-13T13:00:00"),
]

atm_df = spark.createDataFrame(atm_data)

# Read settlement CSV
settlement_df = spark.read \
    .option("header", True) \
    .csv("/settlement/settlement_transactions.csv")

# Cast amount to int
settlement_df = settlement_df.withColumn("amount", col("amount").cast("int"))

# Rename columns to avoid conflicts
atm_df = atm_df.withColumnRenamed("amount", "atm_amount")
settlement_df = settlement_df.withColumnRenamed("amount", "settlement_amount")

# Clean transaction_id (CRITICAL FIX)
atm_df = atm_df.withColumn("transaction_id", trim(col("transaction_id")))
settlement_df = settlement_df.withColumn("transaction_id", trim(col("transaction_id")))

# Full outer join
result_df = atm_df.join(
    settlement_df,
    "transaction_id",
    "full_outer"
)

# Reconciliation logic
result_df = result_df.withColumn(
    "reconciliation_status",
    when(col("atm_amount").isNull(), "MISSING_IN_ATM")
    .when(col("settlement_amount").isNull(), "MISSING_IN_SETTLEMENT")
    .when(col("atm_amount") != col("settlement_amount"), "MISMATCHED")
    .otherwise("MATCHED")
)

# Generate age_days (random 1–4)
result_df = result_df.withColumn(
    "age_days",
    expr("cast(rand()*4 + 1 as int)")
)

# Escalation logic
result_df = result_df.withColumn(
    "escalation_status",
    when(col("age_days") >= 3, "ESCALATED")
    .otherwise("NORMAL")
)

# Debug prints
print("=== ATM DATA ===")
atm_df.show(5, False)

print("=== SETTLEMENT DATA ===")
settlement_df.show(5, False)

print("=== RESULT DATA ===")
result_df.show(20, False)

print("=== CHECK MATCHING IDS ===")
atm_df.select("transaction_id").show(5, False)
settlement_df.select("transaction_id").show(5, False)

print("=== MATCHED ROWS ===")
result_df.filter(col("reconciliation_status") == "MATCHED").show(10, False)

print("=== MISMATCHED ROWS ===")
result_df.filter(col("reconciliation_status") == "MISMATCHED").show(10, False)

print("=== MISSING IN ATM ROWS ===")
result_df.filter(col("reconciliation_status") == "MISSING_IN_ATM").show(10, False)

print("=== MISSING IN SETTLEMENT ROWS ===")
result_df.filter(col("reconciliation_status") == "MISSING_IN_SETTLEMENT").show(10, False)

print("=== ESCALATED ROWS ===")
result_df.filter(col("escalation_status") == "ESCALATED").show(10, False)

print("===== FINAL RECONCILIATION OUTPUT =====")
result_df.groupBy("reconciliation_status").count().show()


# Write output
result_df.write \
    .mode("overwrite") \
    .parquet("/tmp/atm_reconciliation_output")

print("✅ Output written successfully!")

result_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/tmp/recon_output_csv")
print("CSV saved!")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, datediff
import os

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("ATM Reconciliation Job") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/spark/work-dir/warehouse") \
    .getOrCreate()


# -------------------------------
# 2. Configuration
# -------------------------------
ICEBERG_DB = os.getenv("ICEBERG_DB", "atm_db")
ATM_TABLE = f"my_catalog.{ICEBERG_DB}.atm_transactions"
SETTLEMENT_TABLE = f"my_catalog.{ICEBERG_DB}.settlement_records"
OUTPUT_TABLE = f"my_catalog.{ICEBERG_DB}.atm_reconciliation_results"


# -------------------------------
# 3. Read Source Data
# -------------------------------
atm_df = spark.read.format("iceberg").load(ATM_TABLE)

settlement_df = spark.read.format("iceberg").load(SETTLEMENT_TABLE)


# -------------------------------
# 4. Reconciliation Logic
# -------------------------------
recon_df = atm_df.alias("atm").join(
    settlement_df.alias("settlement"),
    col("atm.transaction_id") == col("settlement.transaction_id"),
    "full_outer"
).select(
    col("atm.transaction_id").alias("transaction_id"),
    col("atm.atm_id"),
    col("atm.card_id"),
    col("atm.atm_amount"),
    col("settlement.settlement_amount"),
    col("atm.transaction_time")
)


# -------------------------------
# 5. Status Derivation
# -------------------------------
recon_df = recon_df.withColumn(
    "reconciliation_status",
    when(col("atm_amount").isNull(), "MISSING_IN_ATM")
    .when(col("settlement_amount").isNull(), "MISSING_IN_SETTLEMENT")
    .when(col("atm_amount") != col("settlement_amount"), "AMOUNT_MISMATCH")
    .otherwise("MATCHED")
)


# -------------------------------
# 6. Aging + Escalation Logic
# -------------------------------
recon_df = recon_df.withColumn(
    "age_days",
    datediff(current_timestamp(), col("transaction_time"))
)

recon_df = recon_df.withColumn(
    "escalation_status",
    when(col("age_days") > 2, "ESCALATED")
    .otherwise("NORMAL")
)


# -------------------------------
# 7. Write Output (Iceberg)
# -------------------------------
recon_df.write \
    .format("iceberg") \
    .mode("append") \
    .save(OUTPUT_TABLE)


# 8. Export for dashboard
recon_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/work-dir/recon_output")

# 9. Show output
recon_df.show()

spark.stop()
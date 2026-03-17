from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, current_date, datediff

spark = SparkSession.builder \
    .appName("ATM Reconciliation Job") \
    .getOrCreate()

atm_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/atm_db") \
    .option("dbtable", "atm_transactions") \
    .option("user", "atm_user") \
    .option("password", "atm_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

settlement_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/settlement/settlement_transactions.csv")

print("ATM count:", atm_df.count())
print("Settlement count:", settlement_df.count())

atm_df.show(truncate=False)
settlement_df.show(truncate=False)

joined_df = atm_df.alias("a").join(
    settlement_df.alias("s"),
    col("a.transaction_id") == col("s.transaction_id"),
    "fullouter"
)

print("Joined count:", joined_df.count())
joined_df.show(truncate=False)

result_df = joined_df.select(
    coalesce(col("a.transaction_id"), col("s.transaction_id")).alias("transaction_id"),
    coalesce(col("a.atm_id"), col("s.atm_id")).alias("atm_id"),
    coalesce(col("a.card_id"), col("s.card_id")).alias("card_id"),
    col("a.amount").alias("atm_amount"),
    col("s.amount").alias("settlement_amount"),
    col("a.transaction_time").alias("atm_transaction_time"),
    col("s.transaction_time").alias("settlement_transaction_time"),

    # ✅ Reconciliation status
    when(
        col("a.transaction_id").isNotNull() &
        col("s.transaction_id").isNotNull() &
        (col("a.amount") == col("s.amount")),
        "MATCHED"
    ).when(
        col("a.transaction_id").isNotNull() &
        col("s.transaction_id").isNull(),
        "MISSING_IN_SETTLEMENT"
    ).when(
        col("a.transaction_id").isNull() &
        col("s.transaction_id").isNotNull(),
        "MISSING_IN_ATM"
    ).when(
        col("a.transaction_id").isNotNull() &
        col("s.transaction_id").isNotNull() &
        (col("a.amount") != col("s.amount")),
        "AMOUNT_MISMATCH"
    ).alias("reconciliation_status")
)

# ✅ ADD THIS PART (Aging + Escalation)

result_df = result_df.withColumn(
    "age_days",
    datediff(current_date(), col("atm_transaction_time"))
)

result_df = result_df.withColumn(
    "escalation_status",
    when(col("age_days") <= 1, "NORMAL")
    .when((col("age_days") > 1) & (col("age_days") <= 3), "WARNING")
    .otherwise("ESCALATED")
)

print("Result count:", result_df.count())
result_df.show(truncate=False)

result_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/atm_db") \
    .option("dbtable", "atm_reconciliation_results") \
    .option("user", "atm_user") \
    .option("password", "atm_pass") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Reconciliation completed successfully.")
spark.stop()
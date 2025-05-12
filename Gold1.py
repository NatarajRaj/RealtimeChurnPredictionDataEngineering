from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, datediff, round, abs, hash, lit, max, sum, avg, \
    to_timestamp
import time  # To use the sleep function for delays

from Silver1 import app_df

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("ChurnGoldLayerTransform") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

def process_data():
    """
    # === Calculate 5 minute window cutoff ===
    cutoff_time = expr("current_timestamp() - INTERVAL 5 MINUTES").cast("timestamp")

    # === Load Incremental Silver Data ===

    # For `app_usage` table, ensure `event_time` is treated as a TimestampType
    app_df = spark.read.format("delta").load("D:/data1/delta/silver/app_usage") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .filter(col("event_time") > cutoff_time)

    app_df.show()

    # For `churn_transactions` table, ensure `timestamp` is treated as a TimestampType
    trans_df = spark.read.format("delta").load("D:/data1/delta/silver/churn_transactions") \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .filter(col("timestamp") > cutoff_time)

    trans_df.show()

    # For `customer_profile` table, ensure `event_time` is treated as a TimestampType
    customer_df = spark.read.format("delta").load("D:/data1/delta/silver/customer_profile") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .filter(col("event_time") > cutoff_time)

    customer_df.show()

    # For `support_tickets` table, ensure `created_at` is treated as a TimestampType
    support_df = spark.read.format("delta").load("D:/data1/delta/silver/support_tickets") \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .filter(col("created_at") > cutoff_time)
    support_df.show()
    """
    app_df = spark.read.format("delta").load("D:/data1/delta/silver/app_usage")
    app_df = app_df.withColumn("customer_id", col("customer_id").cast("string"))
    trans_df = spark.read.format("delta").load("D:/data1/delta/silver/churn_transactions")
    trans_df = trans_df.withColumn("customer_id", col("customer_id").cast("string"))
    customer_df = spark.read.format("delta").load("D:/data1/delta/silver/customer_profile")
    customer_df = customer_df.withColumn("customer_id", col("customer_id").cast("string"))
    support_df = spark.read.format("delta").load("D:/data1/delta/silver/support_tickets")
    support_df = support_df.withColumn("customer_id", col("customer_id").cast("string"))

    # === Feature 1: App Behavior ===
    app_features = app_df \
        .withColumn("days_since_last_login", datediff(current_timestamp(), col("last_login"))) \
        .groupBy("customer_id") \
        .agg(
            max("days_since_last_login").alias("login_gap_days"),
            sum("sessions_last_30d").alias("total_sessions_last_30d")
        )

    # === Feature 2: Financial (Declines) ===
    payment_declines = trans_df \
        .filter(col("event_type") == "payment_decline") \
        .groupBy("customer_id") \
        .agg(
            sum("value").alias("total_declined_payments"),
            avg("value").alias("avg_payment_value")
        )

    # === ARPU (Average Rate Per Usage) ===
    arpu_df = trans_df \
        .groupBy("customer_id") \
        .agg(sum("value").alias("total_revenue")) \
        .join(app_features, on="customer_id", how="left") \
        .withColumn("arpu", round(col("total_revenue") / (col("total_sessions_last_30d") + lit(1)), 2))

    # === Feature 3: Support Sentiment ===
    support_features = support_df \
        .withColumn("ticket_sentiment_score", (abs(hash("issue")) % 10) / 10.0) \
        .groupBy("customer_id") \
        .agg(
            avg("resolution_time").alias("avg_resolution_time"),
            avg("ticket_sentiment_score").alias("avg_ticket_sentiment")
        )

    # === Final Enrichment Join ===
    enriched_df = customer_df \
        .join(app_features, on="customer_id", how="left") \
        .join(payment_declines, on="customer_id", how="left") \
        .join(arpu_df.select("customer_id", "arpu"), on="customer_id", how="left") \
        .join(support_features, on="customer_id", how="left") \
        .withColumn("signup_days_ago", datediff(current_timestamp(), col("signup_date"))) \
        .select(
            "customer_id", "name", "email", "age", "gender", "signup_date", "signup_days_ago",
            "login_gap_days", "total_sessions_last_30d",
            "total_declined_payments", "avg_payment_value", "arpu",
            "avg_resolution_time", "avg_ticket_sentiment"
        )

    print(enriched_df)
    enriched_df.show()
    # === Write to Gold Delta Table ===
    enriched_df.write.format("delta") \
        .mode("append") \
        .option("overwriteSchema", "true") \
        .save("D:/data1/delta/gold/enriched_customer_features")

    # === Optionally also push to Kafka ===
    enriched_df.selectExpr("CAST(customer_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "gold.customer_behavior_features") \
        .option("checkpointLocation", "D:/data1/checkpoints/kafka/gold/customer_behavior_features") \
        .save()


    # Collect app_features, payment_declines, arpu, support_features, and enriched_df into Pandas DataFrames
    def gold_dataframes():
        # ... your Spark and feature logic ...
        app_features_pd = app_features.toPandas()
        payment_declines_pd = payment_declines.toPandas()
        arpu_df_pd = arpu_df.toPandas()
        support_features_pd = support_features.toPandas()
        enriched_df_pd = enriched_df.toPandas()

        return {
            "app_features": app_features_pd,
            "payment_declines": payment_declines_pd,
            "arpu": arpu_df_pd,
            "support_features": support_features_pd,
            "enriched_df": enriched_df_pd
        }

# Run the batch process every minute using a while loop
while True:
    data =  process_data()   # collect the data for visualization
    time.sleep(60)

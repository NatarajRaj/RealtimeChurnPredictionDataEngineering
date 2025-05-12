from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("ChurnSilverLayerTransform") \
    .master("local[2]") \
    .config("spark.jars.packages", \
                "io.delta:delta-core_2.12:1.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

def read_kafka(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss","false") \
        .load()

# ===================== 1. CUSTOMER PROFILE =====================
customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("signup_date", StringType()),
    StructField("event_time", TimestampType())
])

customer_df = read_kafka("cdc_dbserver1.public.customer_profile") \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), customer_schema).alias("data")).select("data.*") \
    .withColumn("signup_date", to_date("signup_date")) \
    .dropna(subset=["customer_id", "email"]) \
    .filter(col("age") > 0) \
    .filter(col("gender").isin("Male", "Female", "Other")) \
    .dropDuplicates(["customer_id", "event_time"]) \
    .filter(~col("email").contains("test")) \
    .filter(~col("name").rlike("test|dummy|xyz"))

customer_df.selectExpr("CAST(customer_id AS STRING) AS key",
                       "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "silver_customer_df") \
    .option("checkpointLocation", "/checkpoints/kafka/silver/silver_customer_df") \
    .start()

# ===================== 2. APP USAGE =====================
app_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("last_login", TimestampType()),
    StructField("sessions_last_30d", IntegerType()),
    StructField("event_time", TimestampType())
])

app_df = read_kafka("cdc_dbserver1.public.app_usage") \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), app_schema).alias("data")).select("data.*") \
    .dropna(subset=["customer_id"]) \
    .filter(col("sessions_last_30d") >= 0) \
    .dropDuplicates(["customer_id", "event_time"])

app_df.selectExpr("CAST(customer_id AS STRING) AS key",
                  "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "silver_app_df") \
    .option("checkpointLocation", "/checkpoints/kafka/silver/silver_app_df") \
    .start()

# ===================== 3. CHURN TRANSACTIONS =====================
trans_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("value", DoubleType())
])

trans_df = read_kafka("churn_transactions") \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), trans_schema).alias("data")).select("data.*") \
    .dropna(subset=["customer_id", "timestamp"]) \
    .filter(col("value") > 0) \
    .dropDuplicates(["customer_id", "timestamp"])

trans_df.selectExpr("CAST(customer_id AS STRING) AS key",
                    "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "silver_trans_df") \
    .option("checkpointLocation", "/checkpoints/kafka/silver/silver_trans_df") \
    .start()

# ===================== 4. SUPPORT TICKETS =====================
support_schema = StructType([
    StructField("ticket_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("issue", StringType()),
    StructField("priority", StringType()),
    StructField("status", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
    StructField("assigned_to", StringType()),
    StructField("resolution_time", StringType())
])

support_df = read_kafka("support_tickets") \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), support_schema).alias("data")).select("data.*") \
    .dropna(subset=["customer_id", "ticket_id"]) \
    .filter(col("status").isin("Open", "In Progress", "Resolved", "Closed")) \
    .dropDuplicates(["ticket_id", "updated_at"])

support_df.selectExpr("CAST(ticket_id AS STRING) AS key",
                      "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "silver_support_df") \
    .option("checkpointLocation", "/checkpoints/kafka/silver/silver_support_df") \
    .start()

# ===================== WRITE TO SILVER LAYER =====================

customer_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "D:/data1/checkpoints/silver/customer_profile") \
    .outputMode("append") \
    .start("D:/data1/delta/silver/customer_profile")

app_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "D:/data1/checkpoints/silver/app_usage") \
    .outputMode("append") \
    .start("D:/data1/delta/silver/app_usage")

trans_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "D:/data1/checkpoints/silver/churn_transactions") \
    .outputMode("append") \
    .start("D:/data1/delta/silver/churn_transactions")

support_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "D:/data1/checkpoints/silver/support_tickets") \
    .outputMode("append") \
    .start("D:/data1/delta/silver/support_tickets")

spark.streams.awaitAnyTermination()



import multiprocessing
import json
import time
import random
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from DezbeiumConnetor import PostgresCDCManager

# Kafka Producer Code (run in its own process)
def start_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    event_types = ['purchase', 'refund']

    while True:
        customer_id = f'CUST{random.randint(1, 99999):05d}'

        churn_event = {
            "customer_id": customer_id,
            "event_type": random.choice(event_types),
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
            "value": round(random.uniform(10, 200), 2)
        }
        producer.send('churn_transactions', value=churn_event)

        time.sleep(1)

# Spark Structured Streaming Code (run in its own process)
def start_spark_streaming():
    spark = SparkSession.builder \
        .appName("StreamToDelta") \
        .config("spark.jars.packages", \
                "io.delta:delta-core_2.12:1.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    #  {topic.prefix}{database.server.name}.{schema}.{table} , topic name should be must this name
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "cdc_dbserver1.public.customer_profile,cdc_dbserver1.public.app_usage,churn_transactions,churn_transactions") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "true") \
        .load()

    # Define schemas
    customer_profile_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("age", IntegerType()),
        StructField("gender", StringType()),
        StructField("tenure", IntegerType()),
        StructField("location", StringType())
    ])
    app_usage_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("last_login", StringType()),
        StructField("sessions_last_30d", IntegerType())
    ])
    churn_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType()),
        StructField("value", DoubleType())
    ])

    raw = kafka_stream.selectExpr("CAST(value AS STRING) AS value", "topic")

    profiles = raw.filter(col("topic") == 'cdc_dbserver1.public.customer_profile') \
                 .select(from_json(col("value"), customer_profile_schema).alias("data")) \
                 .select("data.*")

    usage = raw.filter(col("topic") == 'cdc_dbserver1.public.app_usage') \
               .select(from_json(col("value"), app_usage_schema).alias("data")) \
               .select("data.*")

    churn = raw.filter(col("topic") == 'churn_transactions') \
               .select(from_json(col("value"), churn_schema).alias("data")) \
               .select("data.*")

    checkpoint = "D:/data1/checkpoints"
    bronze = "D:/data1/delta/bronze"

    profiles.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint}/profiles") \
        .outputMode("append") \
        .start(f"{bronze}/profiles")

    usage.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint}/usage") \
        .outputMode("append") \
        .start(f"{bronze}/usage")

    usage.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    # usage.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

    churn.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint}/churn") \
        .outputMode("append") \
        .start(f"{bronze}/churn")

    spark.streams.awaitAnyTermination()


# Main block: launch both producer and streaming in parallel
if __name__ == "__main__":

    # Initialize PostgresCDCManager and establish connection
    try:
        conn = PostgresCDCManager()
        conn.connection()
    except Exception as e:
        print(f"Error connecting to PostgresCDCManager: {e}")
        exit(1)

    # Start the Kafka Producer and Spark Streaming in separate processes
    try:
        producer_process = multiprocessing.Process(target=start_kafka_producer)
        producer_process.start()

        streaming_process = multiprocessing.Process(target=start_spark_streaming)
        streaming_process.start()

        # Wait for both processes to finish
        producer_process.join()
        streaming_process.join()

    except Exception as e:
        print(f"Error in multiprocessing: {e}")

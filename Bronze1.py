import json
import time
import random
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from faker import Faker
from datetime import datetime

# Kafka Producer Code (run in its own process)
def start_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    event_types = ['purchase', 'refund']
    genders = ['Male', 'Female']
    support_issues = ['App crashes', 'Transaction failure', 'Account locked', 'Feature not working']

    fake = Faker()

    while True:
        customer_id = f'CUST{random.randint(1, 99999):05d}'

        churn_event = {
            "customer_id": customer_id,
            "event_type": random.choice(event_types),
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
            "value": round(random.uniform(10, 200), 2)
        }

        # Generate customer_profile
        customer_profile = {
            "customer_id": customer_id,
            "name": fake.name(),
            "email": fake.email(),
            "gender": random.choice(genders),
            "age": random.randint(18, 80),
            "signup_date": fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
            "event_time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
            "tenure": random.randint(1, 100),
            "location": fake.city()
        }

        # Generate app_usage
        app_usage = {
            "customer_id": customer_id,
            "last_login": time.strftime('%Y-%m-%d'),
            "sessions_last_30d": random.randint(1, 30)
        }

        # Generate Support Ticket sporadically
        generate_support_ticket = random.random() < 0.05  # 5% chance to generate a support ticket

        if generate_support_ticket:
            support_tickets = {
                "ticket_id": f"ST{random.randint(10000, 99999)}",
                "customer_id": customer_id,
                "issue": random.choice(support_issues),
                "priority": random.choice(['low', 'medium', 'high']),
                "status": "open",
                "created_at": time.strftime('%Y-%m-%dT%H:%M:%S'),
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%S'),
                "assigned_to": f"Agent{random.randint(1, 10)}",
                "resolution_time": None
            }
            producer.send('support_tickets', value=support_tickets)

        # Publish to Kafka
        producer.send('cdc_dbserver1.public.customer_profile', value=customer_profile)
        producer.send('cdc_dbserver1.public.app_usage', value=app_usage)
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
        .option("subscribe", "cdc_dbserver1.public.customer_profile,cdc_dbserver1.public.app_usage,churn_transactions,support_tickets") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "latest") \
        .load()

    # Define schemas
    customer_profile_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType()),
        StructField("signup_date", StringType()),
        StructField("event_time", TimestampType())
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

    support_schema = StructType([
        StructField("ticket_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("issue", StringType()),
        StructField("priority", StringType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at",  TimestampType()),
        StructField("assigned_to", StringType()),
        StructField("resolution_time", StringType())
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

    support = raw.filter(col("topic") == 'support_tickets') \
                 .select(from_json(col("value"), support_schema). alias("data")) \
                 .select("data.*")

    checkpoint = "D:/data1/checkpoints/bronze"
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

    churn.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint}/churn") \
        .outputMode("append") \
        .start(f"{bronze}/churn")

    support.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint}/support") \
        .outputMode("append") \
        .start(f"{bronze}/support")

    # support.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

    spark.streams.awaitAnyTermination()

# Main block: launch both producer and streaming in parallel
# if __name__ == "__main__":
#     multiprocessing.set_start_method("spawn")
#
#     # Start the Kafka Producer and Spark Streaming in separate processes
#     try:
#         producer_process = multiprocessing.Process(target=start_kafka_producer)
#         producer_process.start()
#
#         streaming_process = multiprocessing.Process(target=start_spark_streaming)
#         streaming_process.start()
#
#         # Wait for both processes to finish
#         producer_process.join()
#         streaming_process.join()
#
#     except Exception as e:
#         print(f"Error in multiprocessing: {e}")
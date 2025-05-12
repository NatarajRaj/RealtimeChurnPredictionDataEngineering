import multiprocessing
import subprocess
from Bronze1 import start_kafka_producer, start_spark_streaming

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    try:
        # Start Kafka Producer and Spark Streaming in parallel
        producer_process = multiprocessing.Process(target=start_kafka_producer)
        streaming_process = multiprocessing.Process(target=start_spark_streaming)

        producer_process.start()
        streaming_process.start()

        # Start Silver and Gold scripts concurrently
        silver_process = subprocess.Popen(["python", "Silver1.py"])
        gold_process = subprocess.Popen(["python", "Gold1.py"])

        # Wait for all processes to finish (if needed)
        producer_process.join()
        streaming_process.join()

        # Wait for Silver and Gold pipeline processes
        silver_process.wait()
        gold_process.wait()


    except Exception as e:
        print(f"Error starting processes: {e}")
import uuid
import requests
import psycopg2
import time
from psycopg2 import OperationalError

class PostgresCDCManager:
    def __init__(self):
        # Configuration - Update these values as needed
        self.pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'realtimechrun',
            'user': 'postgres',
            'password': 'Nataraj@123'
        }

        self.kafka_connect_url = "http://localhost:8083/connectors"
        self.headers = {"Content-Type": "application/json"}

        unique_slot_name = f"debezium_slot_{str(uuid.uuid4())[:8]}"

        self.connector_config = {
            "name": "postgres-cdc-connector",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": self.pg_config['host'],
                "database.port": str(self.pg_config['port']),
                "database.user": self.pg_config['user'],
                "database.password": self.pg_config['password'],
                "database.dbname": self.pg_config['database'],
                "database.server.name": "dbserver1",
                "table.include.list": "public.customer_profile,public.app_usage",
                "plugin.name": "pgoutput",
                "slot.name": unique_slot_name,
                "publication.name": "my_publication",
                "snapshot.mode": "always",
                "snapshot.locking.mode": "none",
                "database.history.kafka.bootstrap.servers": "localhost:9092",
                "topic.prefix": "cdc_",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
            }
        }

    def verify_postgres_config(self):
        """Verify PostgreSQL is properly configured for CDC"""
        try:
            conn = psycopg2.connect(**self.pg_config)
            conn.autocommit = True
            cur = conn.cursor()

            # Verify WAL level
            cur.execute("SHOW wal_level")
            if cur.fetchone()[0] != 'logical':
                raise Exception("wal_level must be 'logical'")

            # Verify replication settings
            cur.execute("SHOW max_wal_senders")
            if int(cur.fetchone()[0]) < 10:
                raise Exception("max_wal_senders should be >= 10")

            cur.execute("SHOW max_replication_slots")
            if int(cur.fetchone()[0]) < 10:
                raise Exception("max_replication_slots should be >= 10")

            print("✓ PostgreSQL configuration validated successfully")
            return True

        except OperationalError as e:
            print(f"Connection failed: {e}")
            return False
        except Exception as e:
            print(f"Configuration error: {e}")
            return False
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()

    def deploy_debezium_connector(self, max_retries=5, retry_delay=5):
        """Robust connector deployment with retries and better error handling"""
        if not hasattr(self, 'kafka_connect_url') or not self.kafka_connect_url:
            raise ValueError("kafka_connect_url is not configured")

        if not hasattr(self, 'connector_config') or not self.connector_config:
            raise ValueError("connector_config is not configured")

        health_url = self.kafka_connect_url.replace("/connectors", "")

        for attempt in range(1, max_retries + 1):
            try:
                # 1. Verify Kafka Connect is alive with more detailed checks
                try:
                    print(f"Checking Kafka Connect health... Attempt {attempt}/{max_retries}")
                    health = requests.get(
                        health_url,
                        timeout=5,
                        headers={'Accept': 'application/json'}
                    )

                    if health.status_code != 200:
                        error_msg = (f"Kafka Connect unhealthy (HTTP {health.status_code}) - "
                                     f"Response: {health.text[:200]}")
                        raise ConnectionError(error_msg)

                except requests.exceptions.ConnectionError as ce:
                    print(f"Attempt {attempt}/{max_retries}: Cannot connect to Kafka Connect at {health_url}.")
                    raise ConnectionError(f"Cannot connect to Kafka Connect at {health_url}") from ce

                except requests.exceptions.Timeout as te:
                    print(f"Attempt {attempt}/{max_retries}: Kafka Connect health check timed out.")
                    raise TimeoutError("Kafka Connect health check timed out") from te

                except Exception as e:
                    print(f"Attempt {attempt}/{max_retries}: Health check failed - {str(e)}")
                    raise RuntimeError(f"Health check failed: {str(e)}") from e

                # 2. Attempt deployment with better request handling
                print(f"Attempting to deploy connector... Attempt {attempt}/{max_retries}")
                response = requests.post(
                    self.kafka_connect_url,
                    headers=self.headers,
                    json=self.connector_config,
                    timeout=15  # Increased timeout for initial deployment
                )

                # 3. Enhanced response handling
                if response.status_code == 201:
                    print("✓ Connector deployed successfully")
                    return True

                elif response.status_code == 409:
                    print("⚠ Connector already exists - attempting update...")
                    return self._update_connector_config()

                elif 400 <= response.status_code < 500:
                    error_msg = (f"Client error (HTTP {response.status_code}): "
                                 f"{response.text[:500]}")
                    print(error_msg)
                    return False  # Don't retry client errors

                else:
                    error_msg = (f"Unexpected response (HTTP {response.status_code}): "
                                 f"{response.text[:500]}")
                    print(error_msg)
                    raise RuntimeError(error_msg)

            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt}/{max_retries} failed: {str(e)}")
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 60)  # Exponential backoff with a cap
                continue

            except Exception as e:
                print(f"Critical error during attempt {attempt}: {str(e)}")
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 60)  # Exponential backoff with a cap
                continue

        print(f"❌ Failed after {max_retries} attempts")
        return False

    def _update_connector_config(self):
        """Enhanced connector configuration update"""
        if not hasattr(self, 'connector_config') or not self.connector_config:
            raise ValueError("connector_config is not configured")

        update_url = f"{self.kafka_connect_url}/{self.connector_config['name']}/config"

        try:
            response = requests.put(
                update_url,
                headers=self.headers,
                json=self.connector_config['config'],
                timeout=15
            )

            if response.status_code == 200:
                print("✓ Connector configuration updated successfully")
                return True

            elif response.status_code == 404:
                print("⚠ Connector not found - cannot update")
                return False

            else:
                error_msg = (f"Update failed (HTTP {response.status_code}): "
                             f"{response.text[:500]}")
                print(error_msg)
                return False

        except requests.exceptions.RequestException as e:
            print(f"Update request failed: {str(e)}")
            return False

        except Exception as e:
            print(f"Unexpected error during update: {str(e)}")
            return False

    def monitor_connector(self, timeout_minutes=5):
        """Monitor the connector status"""
        start_time = time.time()
        connector_name = self.connector_config['name']

        while time.time() - start_time < timeout_minutes * 60:
            try:
                response = requests.get(f"{self.kafka_connect_url}/{connector_name}/status")
                status = response.json()

                connector_state = status['connector']['state']
                task_state = status['tasks'][0]['state']

                print(f"Connector status: {connector_state}, Task status: {task_state}")

                if connector_state == 'RUNNING' and task_state == 'RUNNING':
                    print("✓ Connector is running successfully")
                    return True

                if connector_state == 'FAILED' or task_state == 'FAILED':
                    print("✗ Connector failed")
                    print(f"Error: {status['tasks'][0]['trace']}")
                    return False

                time.sleep(5)

            except Exception as e:
                print(f"Monitoring error: {e}")
                time.sleep(10)

        print("Timeout reached while monitoring connector")
        return False

    def run(self):
        """Execute the complete CDC setup process"""
        if not self.verify_postgres_config():
            return False

        if not self.deploy_debezium_connector():
            return False

        if not self.monitor_connector():
            return False

        return True

    def connection(self):
        cdc_manager = PostgresCDCManager()

        if cdc_manager.run():
            print("CDC setup completed successfully!")
        else:
            print("CDC setup failed")

# start zookeeper, kafka server
# C:\kafka\bin\windows\connect-distributed.bat C:\kafka\config\connect-distributed.properties
# dezbeium postgres jar download added to the plugins path and also mention plugins path to the connect-distributed-properties

#  verify on the information in kafka/config/connect-distributed.properties,  kafka/config/connect-standalone.properties
"""
bootstrap.servers=localhost:9092
rest.port=8083
rest.advertised.host.name=localhost
plugin.path=C:/kafka/plugins
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
group.id=connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
"""
import os
import time
from minio import Minio
from minio.error import S3Error
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

s3_endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio:9000").replace("http://","").replace("https://","")
bucket = os.getenv("S3_BUCKET", "alpha")

client = Minio(
    s3_endpoint,
    access_key=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
    secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin"),
    secure=False
)

# Create S3 bucket if not exists
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Created bucket {bucket}")
else:
    print(f"Bucket {bucket} exists")

# Create Kafka topics
brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
ticks_topic = os.getenv("KAFKA_TICKS_TOPIC", "ticks")
bars_topic = os.getenv("KAFKA_BARS_TOPIC", "minute_bars")

for i in range(30):
    try:
        admin = KafkaAdminClient(bootstrap_servers=brokers, client_id="init")
        break
    except Exception as e:
        print("Waiting for Kafka...", e)
        time.sleep(2)
else:
    raise SystemExit("Kafka unavailable")

topics = [NewTopic(name=ticks_topic, num_partitions=1, replication_factor=1),
          NewTopic(name=bars_topic, num_partitions=1, replication_factor=1)]
try:
    admin.create_topics(new_topics=topics, validate_only=False)
    print("Created topics")
except Exception as e:
    print("Topics may already exist:", e)

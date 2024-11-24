# Kafka producer to send processed data
from confluent_kafka import Producer
import json
from datetime import datetime
from utils import setup_logger
import uuid
logger = setup_logger()

KAFKA_BROKER = "localhost:29092"
def random_id():
    return str(uuid.uuid4())

data = {
    "user_id": random_id(),
    "app_version": "2.0.1",
    "locale": "US",
    "device_id":random_id(),
    "timestamp": datetime.now(),
    "device_type": "android"
}

def produce_message(data, topic):
    """Produces a message to a Kafka topic."""
    producer_config = {"bootstrap.servers": KAFKA_BROKER}
    producer = Producer(producer_config)

    def delivery_report(err, msg):
        """Delivery report callback."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        # Handle non-serializable fields (like datetime)
        if isinstance(data.get("timestamp"), datetime):
            data["timestamp"] = data["timestamp"].isoformat()

        json_data = json.dumps(data)

        producer.produce(topic, value=json_data, callback=delivery_report)
        producer.flush()  # Ensure the message is sent
        logger.info(f"Produced message to {topic}: {data}")
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")

produce_message(data, "processed-user-login")
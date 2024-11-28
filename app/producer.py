# Kafka producer to send processed data
from confluent_kafka import Producer
import json
from datetime import datetime

from utils import setup_logger
logger = setup_logger()

KAFKA_BROKER = "localhost:29092"

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
from confluent_kafka import Producer
import json
from datetime import datetime
from logging_config import setup_logger
from time import sleep

KAFKA_BROKER = "localhost:29092"
producer = Producer({"bootstrap.servers": KAFKA_BROKER})
logger = setup_logger()

def produce_message(data, topic, key=None, max_retries=3):
    """
    Produces a message to a Kafka topic with retry logic.
    
    :param data: The message payload (dict)
    :param topic: The Kafka topic to which the message will be sent
    :param key: (Optional) The key for partitioning messages
    :param max_retries: Maximum number of retry attempts
    """
    def delivery_report(err, msg):
        """Delivery report callback."""
        if err is not None:
            logger.error(f"Message delivery to {msg.topic()} failed: {err}")

    try:
        # Serialize the message
        if isinstance(data.get("timestamp"), datetime):
            data["timestamp"] = data["timestamp"].isoformat()

        try:
            json_data = json.dumps(data)
        except TypeError as e:
            logger.error(f"Payload serialization failed: {e}. Payload: {data}")
            return

        # Retry logic with backoff
        for attempt in range(1, max_retries + 1):
            try:
                producer.produce(
                    topic, key=str(key) if key else None, value=json_data, callback=delivery_report
                )
                producer.flush()
                logger.info(f"Produced message to {topic}: {data}")
                break
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    sleep(1)  # Backoff before retrying
                else:
                    logger.error(f"Failed to produce message to {topic} after {max_retries} attempts. Payload: {data}")
                    raise
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")
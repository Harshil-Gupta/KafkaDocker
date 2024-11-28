from confluent_kafka import Producer
import json
from datetime import datetime
from logging_config import setup_logger

logger = setup_logger()

KAFKA_BROKER = "localhost:29092"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

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
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        if isinstance(data.get("timestamp"), datetime):
            data["timestamp"] = data["timestamp"].isoformat()

        json_data = json.dumps(data)

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
                if attempt == max_retries:
                    raise
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")
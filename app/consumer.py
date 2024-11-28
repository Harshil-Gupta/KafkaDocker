# Kafka consumer for reading and processing data
import json
from confluent_kafka import Consumer, KafkaError
from producer import produce_message
from utils import process_message, setup_logger

logger = setup_logger()

KAFKA_BROKER = "localhost:29092"
SOURCE_TOPIC = "user-login"
DESTINATION_TOPIC = "processed-user-login"
GROUP_ID = "consumer-group-1"

def consume_messages():
    """Consumes messages from a Kafka topic and processes them."""
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # Start reading at the earliest message
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([SOURCE_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition event
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break

            # Process the incoming message
            try:
                message = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message: {message}")
                processed_message = process_message(message)
                if processed_message:
                    produce_message(processed_message, DESTINATION_TOPIC)
            except json.JSONDecodeError as e:
                logger.error(f"Message decoding failed: {e}")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()

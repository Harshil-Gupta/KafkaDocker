# Kafka consumer for reading and processing data
import json
from confluent_kafka import Consumer, KafkaError
from producer import produce_message
from utils import process_message, setup_logger

logger = setup_logger()

KAFKA_BROKER = "localhost:29092"
SOURCE_TOPIC = "source-user-login"  # Updated for consistent naming
PROCESSED_TOPIC = "processed-user-login"  # Updated for consistent naming
DLQ_TOPIC = "dlq-user-login"  # Updated for consistent naming
GROUP_ID = "consumer-group-1"

def consume_messages(topic):
    """
    Consumes messages from a Kafka topic and processes them.
    Supports both the main source topic and the DLQ topic.
    
    :param topic: The Kafka topic to consume from
    """
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # Start reading at the earliest message
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

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
                logger.info(f"Received message from {topic}: {message}")

                if topic == DLQ_TOPIC:
                    # Special handling for DLQ messages
                    logger.warning(f"Processing message from {topic}: {message}")
                    # Retry processing the message
                    retry_message_processing(message)
                else:
                    # Normal processing for source topic messages
                    processed_message = process_message(message)
                    if processed_message:
                        produce_message(processed_message, PROCESSED_TOPIC)
            except json.JSONDecodeError as e:
                logger.error(f"Message decoding failed: {e}")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()

def retry_message_processing(message):
    """
    Retries processing a message from the DLQ topic.
    - Attempts to reprocess the message.
    - Sends it back to the processed topic if successful.
    """
    try:
        logger.info(f"Retrying message processing: {message}")
        processed_message = process_message(message)
        if processed_message:
            produce_message(processed_message, PROCESSED_TOPIC)
            logger.info(f"Message successfully reprocessed and sent to {PROCESSED_TOPIC}")
        else:
            logger.warning(f"Message processing failed again: {message}")
    except Exception as e:
        logger.error(f"Failed to reprocess message from DLQ: {e}")

if __name__ == "__main__":
    print("Kafka Consumer")
    print("1. Consume from DLQ topic")
    print("2. Consume from source topic")
    choice = input("Enter your choice (1 or 2): ").strip()

    if choice == "1":
        logger.info("Consuming messages from DLQ topic")
        consume_messages(DLQ_TOPIC)
    elif choice == "2":
        logger.info("Consuming messages from source topic")
        consume_messages(SOURCE_TOPIC)
    else:
        logger.error("Invalid choice. Exiting.")
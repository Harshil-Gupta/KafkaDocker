import json
from confluent_kafka import Consumer, KafkaError
from producer import produce_message
from utils import process_message, setup_logger
from time import sleep

logger = setup_logger()

KAFKA_BROKER = "localhost:29092"
SOURCE_TOPIC = "source-user-login"
PROCESSED_TOPIC = "processed-user-login"
DLQ_TOPIC = "dlq-user-login"
GROUP_ID = "consumer-group-1"

def validate_kafka_config():
    """Validates Kafka configuration and logs errors for missing values."""
    required_config = {
        "KAFKA_BROKER": KAFKA_BROKER,
        "SOURCE_TOPIC": SOURCE_TOPIC,
        "PROCESSED_TOPIC": PROCESSED_TOPIC,
        "DLQ_TOPIC": DLQ_TOPIC,
        "GROUP_ID": GROUP_ID,
    }
    for key, value in required_config.items():
        if not value:
            logger.error(f"Missing Kafka configuration: {key} is not set.")
            exit(1)

def process_topic_message(topic, message):
    """
    Processes a message based on the topic.
    Handles normal processing or DLQ retries as needed.
    """
    if topic == DLQ_TOPIC:
        logger.warning(f"Processing message from DLQ: {message}")
        retry_message_processing(message)
    else:
        processed_message = process_message(message)
        if processed_message:
            produce_message(processed_message, PROCESSED_TOPIC)

def consume_messages(topic):
    """
    Consumes messages from a Kafka topic and processes them.
    Supports both the main source topic and the DLQ topic.
    """
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}. Continuing...")
                    continue

            # Process the message
            try:
                message = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message from {topic}: {message}")
                process_topic_message(topic, message)
            except json.JSONDecodeError as e:
                logger.error(f"Message decoding failed: {e}")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        logger.info("Closing Kafka consumer...")
        consumer.close()
        logger.info("Kafka consumer closed.")

def retry_message_processing(message, retry_delay=1):
    """
    Retries processing a message from the DLQ topic with a delay.
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
    finally:
        sleep(retry_delay)

if __name__ == "__main__":
    validate_kafka_config()
    
    print("Kafka Consumer")
    print("1. Consume from DLQ topic")
    print("2. Consume from source topic")
    choice = input("Enter your choice (1 or 2): ").strip()

    if choice == "1":
        logger.info("User selected to consume from DLQ topic.")
        consume_messages(DLQ_TOPIC)
    elif choice == "2":
        logger.info("User selected to consume from source topic.")
        consume_messages(SOURCE_TOPIC)
    else:
        logger.error("Invalid choice. Exiting.")
import logging
from datetime import datetime
from logging_config import setup_logger
from producer import produce_message
import ipaddress

logger = setup_logger()

def extract_message(message):
    """
    Extracts and validates the required fields from the incoming message.
    Ensures data integrity by checking for required fields and types.
    Logs and removes unexpected fields.
    """
    required_fields = {
        "user_id": str,
        "app_version": str,
        "device_type": str,
        "device_id": str,
        "locale": str,
        "ip": str, 
        "timestamp": int,  # Expected type for timestamp
    }
    
    try:
        # Check for missing or invalid fields
        extracted_message = {}
        for field, field_type in required_fields.items():
            if field not in message:
                raise KeyError(f"Missing required field: {field}")
            if not isinstance(message[field], field_type):
                raise TypeError(f"Field '{field}' expected {field_type}, got {type(message[field])}")
            # Special validation for the "ip" field
            if field == "ip":
                try:
                    ipaddress.ip_address(message[field])
                except ValueError:
                    raise ValueError(f"Invalid IP address: {message[field]}")
            
            extracted_message[field] = message[field]
        
        # Identify and log extra fields
        extra_fields = set(message.keys()) - set(required_fields.keys())
        if extra_fields:
            logger.warning(f"Extra fields found and removed: {extra_fields}")
        
        return extracted_message
    except (KeyError, TypeError) as e:
        logger.error(f"Extraction failed: {e}")
        return None


def transform_message(message):
    """
    Transforms the data:
    - Filters out messages with locale 'RU'.
    - Converts timestamp to human-readable format.
    - Applies additional transformations if needed.
    """
    try:
        # Custom Filter logic: Skip messages from locale 'RU'
        if message.get("locale") == "RU":
            logger.info(f"Message filtered out due to locale 'RU': {message}")
            return None

        # Convert timestamp to ISO 8601 human-readable format 
        # Not required
        # message["timestamp"] = datetime.fromtimestamp(
        #     int(message["timestamp"])
        # ).strftime('%Y-%m-%d %H:%M:%S')

        # Example: Enrich the message (adding a new field)
        message["processed_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return message
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return None

def load_message(message, destination, max_retries=3, dlq_topic = "dlq-topic"):
    """
    Loads the processed message into the specified destination (Kafka topic).
    If loading fails after retries, routes the message to a Dead Letter Queue (DLQ).

    :param message: The processed message (dict)
    :param destination: The Kafka topic to which the message will be sent
    :param max_retries: Maximum number of retry attempts
    """
    try:
        produce_message(data=message, topic=destination, max_retries=max_retries)
    except Exception as e:
        logger.error(f"Failed to load message to {destination} after {max_retries} retries: {e}")
        # Route to undeliverable message to Dead Letter Queue (DLQ)
        try:
            logger.info(f"Routing message to DLQ: {dlq_topic}")
            produce_message(data=message, topic=dlq_topic)
        except Exception as dlq_error:
            logger.critical(f"Failed to route message to DLQ: {dlq_error}")

def process_message(message):
    """
    Complete ETL pipeline:
    - Extract, transform, and load the incoming message.
    - Handle errors gracefully during each stage.
    - Fallback to DLQ in case of error message
    """
    try:
        # Extract phase
        extracted_message = extract_message(message)
        if not extracted_message:
            logger.info("Routing invalid message to DLQ")
            load_message(message, destination="dlq-topic")
            return None

        # Transform phase
        transformed_message = transform_message(extracted_message)
        if not transformed_message:
            logger.info("Routing invalid message to DLQ")
            load_message(message, destination="dlq-topic")
            return None

        # Load phase
        load_message(transformed_message,"processed-user-login",max_retries=3) 
        return transformed_message
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        return None

if __name__ == "__main__":
    from uuid import uuid4

    test_cases = [
        # Valid Message
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
            "ip": "16.108.69.154",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "android",
        },
        # Edge Case Timestamp
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
            "ip": "16.108.69.154",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 0,
            "device_type": "android",
        },
        # Missing Fields
        {
            "user_id": str(uuid4()),
            "locale": "IN",
            "timestamp": 1732791315,
        },
        # Invalid Timestamp
        {
            "user_id": str(uuid4()),
            "app_version": "3.0.0",
            "ip": "16.108.69.154",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": "not-a-timestamp",
            "device_type": "android",
        },
        # Invalid IP Address
        {
            "user_id": str(uuid4()),
            "app_version": "3.0.0",
            "ip": "16.108.69.15454",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": "not-a-timestamp",
            "device_type": "android",
        },
        # Filtered Locale [In case, we want to add custom transformation logic to skip particular data]
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
            "ip": "16.108.69.154",
            "locale": "RU",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "android",
        },
        # Empty Message
        {},
        # Extra Fields
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
            "ip": "16.108.69.15454",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "ios",
            "extra_field": "unexpected_value",
        },
        # Invalid Field Type
        {
            "user_id": 12345,
            "app_version": "2.0.0",
            "ip": "16.108.69.15454",
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "android",
        },
    ]

    for i, case in enumerate(test_cases, 1):
        logger.info(f"Running test case {i}")
        process_message(case)
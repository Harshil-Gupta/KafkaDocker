import logging
from datetime import datetime

def setup_logger():
    """Sets up a logger for consistent logging."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger("KafkaPipeline")

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
        "locale": str,
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
        # Filter logic: Skip messages from locale 'RU'
        if message.get("locale") == "RU":
            logger.info(f"Message filtered out due to locale 'RU': {message}")
            return None

        # Convert timestamp to ISO 8601 human-readable format
        message["timestamp"] = datetime.fromtimestamp(
            int(message["timestamp"])
        ).strftime('%Y-%m-%d %H:%M:%S')

        # Example: Enrich the message (adding a new field)
        message["processed_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        return message
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return None

def load_message(message, destination):
    """
    Simulates persisting the processed data into the destination.
    This could be a database, another Kafka topic, or any storage.
    """
    try:
        # In a real-world scenario, this would involve producing the message to Kafka
        # or persisting it into a database.
        logger.info(f"Message loaded to {destination}: {message}")
    except Exception as e:
        logger.error(f"Loading failed: {e}")

def process_message(message):
    """
    Complete ETL pipeline:
    - Extract, transform, and load the incoming message.
    - Handle errors gracefully during each stage.
    """
    try:
        # Extract phase
        extracted_message = extract_message(message)
        if not extracted_message:
            return None

        # Transform phase
        transformed_message = transform_message(extracted_message)
        if not transformed_message:
            return None

        # Load phase
        load_message(transformed_message, "processed-user-login")
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
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "android",
        },
        # Edge Case Timestamp
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
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
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": "not-a-timestamp",
            "device_type": "android",
        },
        # Filtered Locale
        {
            "user_id": str(uuid4()),
            "app_version": "1.0.0",
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
            "locale": "US",
            "device_id": str(uuid4()),
            "timestamp": 1732791315,
            "device_type": "android",
        },
    ]

    for i, case in enumerate(test_cases, 1):
        logger.info(f"Running test case {i}")
        process_message(case)
#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaProducer

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import create_kafka_producer

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Categorize and Process Messages
#####################################

def categorize_and_process_message(message: str, producer: KafkaProducer):
    """
    Categorize messages and log/send to appropriate topics.

    Args:
        message (str): The message to process.
        producer (KafkaProducer): The Kafka producer instance to forward categorized messages.
    """
    if "20" in message:
        category = "PROMO"
        topic = "promo_messages"
    else:
        category = "General Announcement"
        topic = "general_announcements"

    logger.info(f"Categorized as {category}: {message}")

    try:
        producer.send(topic, value=message.encode("utf-8"))
        logger.info(f"Forwarded to topic '{topic}': {message}")
    except Exception as e:
        logger.error(f"Failed to send message to topic '{topic}': {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Categorizes and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer and producer
    consumer = create_kafka_consumer(topic, group_id)
    producer = create_kafka_producer()

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value.decode("utf-8")
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            categorize_and_process_message(message_str, producer)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        producer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

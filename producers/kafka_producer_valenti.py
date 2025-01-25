"""
kafka_producer_case.py

Produce some streaming buzz strings and send them to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1)) # default to 60 seconds
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################


def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of buzz messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.

    """
    string_list: list = [
        "Come play at the Northern Lights Arcade, free tokens to first 20 Guests!",
        "Come play at the Northern Lights Arcade, black light dance party tonight!"
        "Come shop at the Bear's Den GiftShop, free stickers to first 20 Guests!",
        "Come shop at the Bear's Den GiftShop, grab a book and read by the pool!"
        "Come golf at the Howl in One, free game to first 20 Guests",
        "Come golf at the Howl in One, so many colors of balls to choose from!",
        "Come swim at the Waterpark, free waterproof cameras to first 20 Guests!",
        "Come swim at the waterpark, not responsible for damage to non-waterproof cameras!"
    ]

    
    # Separate strings into two categories
    messages_with_20 = [msg for msg in string_list if "20" in msg]
    messages_without_20 = [msg for msg in string_list if "20" not in msg]

    try:
        # to send messages with "20" first
        logger.info("Sending messages containing '20'...")
        for _ in range(20):  # Repeat this for 20 minutes
        for message in messages_with_20:
                logger.info(f"Generated buzz: {message}")
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)

    # Send remaining messages
        logger.info("Sending remaining messages...")
        while True:
            for message in messages_without_20:
                logger.info(f"Generated buzz: {message}")
                producer.send(topic, value=message.encode("utf-8"))
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)  

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated buzz message strings to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

from confluent_kafka import Producer
import random
from time import sleep
import json
import argparse
import logging

from config.data_generation import CONFIG_DATA_GEN
from config.logger import set_logger
from utils import read_cloud_config

set_logger()


def get_args():
    """Parse command-line args

    Returns:
        Namespace : namespace object with command-line arguments
    """
    parser = argparse.ArgumentParser(description="Produces random IoT sensor data")
    parser.add_argument(
        "-n",
        "--no-of-data",
        required=True,
        help="No Of data to be sent to a kafka topic",
    )
    parser.add_argument(
        "-t", "--topic", required=True, help="topic to which the data to be sent"
    )

    return parser.parse_args()


class CustomProducer:
    """Produce some sample IoT data and push it to iot_logs Kafka topic"""

    def __init__(self, topic) -> None:
        self.current_topic = topic

    @staticmethod
    def get_latitude() -> float:
        config_latitude = CONFIG_DATA_GEN["latitude"]
        return round(
            random.uniform(config_latitude["start"], config_latitude["end"]), 4
        )

    @staticmethod
    def get_longitude() -> float:
        config_longitude = CONFIG_DATA_GEN["longitude"]
        return round(
            random.uniform(config_longitude["start"], config_longitude["end"]), 4
        )

    @staticmethod
    def get_vibration_level(type=0) -> float:
        config_vibration = CONFIG_DATA_GEN["vibration"]
        if type == 0:
            start, end = config_vibration["negative"].values()
        else:
            start, end = config_vibration["positive"].values()
        return round(random.uniform(start, end), 2)

    @staticmethod
    def get_acceleration(type=0) -> float:
        config_acceleration = CONFIG_DATA_GEN["acceleration"]
        if type == 0:
            start, end = config_acceleration["negative"].values()
        else:
            start, end = config_acceleration["positive"].values()
        return round(random.uniform(start, end), 2)

    def gather_details(self, index) -> str:
        """Gather all the sensors data and return it as a comma separated value

        Args:
            index (int): index to process - random

        Returns:
            str: comma separated IoT data
        """
        if index % 2 == 0:
            return ",".join(
                map(
                    str,
                    (
                        self.get_latitude(),
                        self.get_longitude(),
                        self.get_vibration_level(1),
                        self.get_acceleration(1),
                    ),
                )
            )
        else:
            return ",".join(
                map(
                    str,
                    (
                        self.get_latitude(),
                        self.get_longitude(),
                        self.get_vibration_level(),
                        self.get_acceleration(),
                    ),
                )
            )

    def set_kafka_configs(self) -> None:
        """Setting up the Kafka producer"""
        self.producer = Producer(read_cloud_config("./client.properties"))

    def send_data(self, key, value) -> None:
        """Send data to a Kafka topic with the producer object

        Args:
            key (str): key to pass to the topic - used in topic partition
            value (str): value to store in the topic
        """
        self.producer.produce(self.current_topic, key=key, value=value)

    def start_producer(self, no_data=1) -> None:
        logging.info("Attempting to set kafka configs")
        self.set_kafka_configs()
        logging.info("Successfully set Kafka configs!")

        # convert data to int if not already
        if type(no_data) != int:
            no_data = int(no_data)

        logging.info(f"Starting the producer on topic - {self.current_topic}")
        # generate data
        for index in range(0, no_data):
            value = self.gather_details(index)
            self.send_data("", value)
            logging.info("Data sent..")
            sleep(0.5)

        # make sure the Kafka topic has acknowledged all the data sent
        self.producer.flush()

        logging.info("Done producing data!")


def main():
    args = get_args()
    logging.info(
        f"Received args - topic - {args.topic} - no_of_data - {args.no_of_data}"
    )

    try:
        producer = CustomProducer(args.topic)
        producer.start_producer(args.no_of_data)
    except Exception as e:
        logging.exception("Error in main:")


if __name__ == "__main__":
    main()

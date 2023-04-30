import logging
from confluent_kafka import Consumer
import argparse
from twilio.rest import Client
import os

from utils import read_cloud_config
from config.logger import set_logger

set_logger()


def get_args():
    parser = argparse.ArgumentParser(
        description="Consume IoT sensor data from a single Kafka topic to make a call"
    )
    parser.add_argument("-t", "--topic", required=True, help="topic to subscribe to")

    return parser.parse_args()


class CallConsumer:
    def __init__(self, topic) -> None:
        self.current_topic = topic
        self.twilio_client = Client()

    def set_consumer(self):
        props = read_cloud_config("./client.properties")
        props["group.id"] = "iot-consumer-group-1"
        props["auto.offset.reset"] = "earliest"

        logging.info("Attempting to start consumer")

        self.consumer = Consumer(props)
        self.consumer.subscribe([self.current_topic])

        logging.info("Connected consumer")

    def trigger_call(self, name="santhosh", child_name="sandy"):
        twiml_url = f"{os.environ.get('TWILIO_TWIML_BIN_URL')}?name={name}&childName={child_name}&lastSeen=10&areaName=Cubbon%20Park"
        logging.info("Attempting to make a call")
        call = self.twilio_client.calls.create(
            from_="+16203902190",
            to="+919003939495",
            url=twiml_url,
        )
        logging.info("Successfully triggered call!")

    def start_consumer(self):
        self.set_consumer()

        logging.info("Started consumer")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    value = msg.value().decode("utf-8")
                    logging.info(f"Received value - {value}")
                    self.trigger_call()

        except KeyboardInterrupt:
            logging.error("keyboard interrupt made")
        except Exception as e:
            logging.error(f"Error in polling - {e}")
        finally:
            self.consumer.close()


def main():
    args = get_args()
    logging.info(f"Received args - topic - {args.topic}")

    try:
        consumer = CallConsumer(args.topic)
        consumer.start_consumer()
    except Exception as e:
        logging.exception("Error in main:")


if __name__ == "__main__":
    main()

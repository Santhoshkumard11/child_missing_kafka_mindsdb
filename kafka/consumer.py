import logging
from confluent_kafka import Consumer, Producer
import argparse
from sqlalchemy import create_engine
import os
import json

from utils import read_cloud_config, send_data_to_mysql, predict_missing
from config.logger import set_logger

set_logger()


def get_args():
    parser = argparse.ArgumentParser(
        description="Consume IoT sensor data from a single Kafka topic"
    )
    parser.add_argument("-t", "--topic", required=True, help="topic to subscribe to")

    return parser.parse_args()


class CustomConsumer:
    def __init__(self, topic) -> None:
        self.current_topic = topic

    def set_consumer(self):
        props = read_cloud_config("./client.properties")
        props["group.id"] = "iot-consumer-group-1"
        props["auto.offset.reset"] = "earliest"

        logging.info("Attempting to start consumer")

        self.consumer = Consumer(props)
        self.consumer.subscribe([self.current_topic])

        logging.info("Connected consumer")

    def create_mysql_connection(self):
        user = os.environ.get("MYSQL_USERNAME")
        password = os.environ.get("MYSQL_PASSWORD")
        host = os.environ.get("MYSQL_HOST")
        port = 3306
        database = "child_missing"
        logging.info("Attempting to create MySQL connection")
        # this might not be an ideal solution - temp fix
        connect_args = {"ssl": {"fake_flag_to_enable_tls": True}}
        return create_engine(
            url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
            connect_args=connect_args,
        ).connect()

    def create_mindsdb_connection(self):
        user = os.environ.get("MINDSDB_USERNAME")
        password = os.environ.get("MINDSDB_PASSWORD")
        host = "cloud.mindsdb.com"
        port = 3306
        database = "mindsdb"
        logging.info("Attempting to create mindsdb connection")
        return create_engine(
            url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        ).connect()

    def send_data_call_topic(self, device_id="12G40D9FT3328HS428"):
        self.producer = Producer(read_cloud_config("./client.properties"))
        data = json.dumps({"device_id": device_id})
        self.producer.produce("call_initiate", key="", value=data)
        # make sure the data is sent and acknowledged
        self.producer.flush()
        logging.info("Message sent to call_initiate topic")

    def check_missing(self, data):
        missing, json_missing = predict_missing(data, self.mindsdb_conn)

        if type(missing) != int:
            missing = int(missing)

        if missing == 1:
            logging.info("Sending data to call topic to initiate call")
            self.send_data_call_topic()

    def start_consumer(self):
        self.set_consumer()
        self.mysql_conn = self.create_mysql_connection()
        self.mindsdb_conn = self.create_mindsdb_connection()

        logging.info("Started consumer")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    value = msg.value().decode("utf-8")
                    logging.info(f"Received value - {value}")
                    send_data_to_mysql(value, self.mysql_conn)
                    self.check_missing(value)

        except KeyboardInterrupt:
            logging.error("keyboard interrupt made")
        except Exception as e:
            logging.error(f"Error in polling - {e}")
        finally:
            self.consumer.close()
            self.mysql_conn.commit()
            self.mysql_conn.close()
            self.mindsdb_conn.close()


def main():
    args = get_args()
    logging.info(f"Received args - topic - {args.topic}")

    try:
        consumer = CustomConsumer(args.topic)
        consumer.start_consumer()
    except Exception as e:
        logging.exception("Error in main:")


if __name__ == "__main__":
    main()

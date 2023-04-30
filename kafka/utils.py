from sqlalchemy import create_engine, text
import os
import logging

from config.logger import set_logger

set_logger()

user = os.environ.get("MYSQL_USERNAME")
password = os.environ.get("MYSQL_PASSWORD")
host = os.environ.get("MYSQL_HOST")
port = 3306
database = "child_missing"


def read_cloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


def create_connection():
    logging.info("Attempting to create a connection")
    return create_engine(
        url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    ).connect()


def generate_query_str(data):
    
    return ""


def send_data_to_mysql(data):
    try:
        conn = create_connection()

        str_query = generate_query_str(data)

        conn.execute(text(str_query))
    except Exception as e:
        logging.error(f"Error in send_data_to_mysql - \n{e}")

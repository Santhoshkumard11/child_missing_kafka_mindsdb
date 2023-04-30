from sqlalchemy import text
import logging


from config.logger import set_logger
from constants import QUERY_INSERT_TEMPLATE, QUERY_PREDICTION

set_logger()


def read_cloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


def generate_query_str(data):
    return QUERY_INSERT_TEMPLATE.format(data)


def is_valid_data(data):
    return data.__len__() != 4


def send_data_to_mysql(data, conn):
    try:
        # skip if the data doesn't match
        if is_valid_data(data):
            str_query = generate_query_str(data)

            conn.execute(text(str_query))
            conn.commit()
            logging.info("committed value")
        else:
            logging.info("Skipping this value")
    except Exception as e:
        logging.error(f"Error in send_data_to_mysql - \n{e}")


def predict_missing(data, conn):
    data = list(map(float, data.split(",")))
    str_query = QUERY_PREDICTION.format(*data)

    missing, json_missing_explain = conn.execute(text(str_query)).fetchone()
    logging.info(f"Predicted class - {missing}")
    return missing, json_missing_explain

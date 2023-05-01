from sqlalchemy import text
import logging
from typing import Union

from config.logger import set_logger
from constants import QUERY_INSERT_TEMPLATE, QUERY_PREDICTION

set_logger()


def read_cloud_config(config_file) -> dict:
    """Read the Kafka conigs

    Args:
        config_file (str): config file name

    Returns:
        dict: configuration of Kafka
    """
    conf = {}
    with open(config_file) as fh:
        # iterate through all the lines
        for line in fh:
            line = line.strip()
            # skip if the first chart is # or it's empty line
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


def generate_query_str(data) -> str:
    """Format MySQL query to insert IoT data

    Args:
        data (list): IoT data

    Returns:
        str: MySQL insert query string
    """
    return QUERY_INSERT_TEMPLATE.format(data)


def is_valid_data(data) -> bool:
    """Validate if the data has four values

    Args:
        data (list): IoT Data

    Returns:
        bool: validate flag
    """
    return data.__len__() != 4


def send_data_to_mysql(data, conn) -> None:
    """Execute the MySQL insert query

    Args:
        data (list): feature values for ML model from IoT device
        conn (Connection): MySQL connection object
    """
    try:
        # skip if the data doesn't match the desired length
        if is_valid_data(data):
            str_query = generate_query_str(data)

            conn.execute(text(str_query))
            conn.commit()
            logging.info("committed value")
        else:
            logging.info("Skipping this value")
    except Exception as e:
        logging.error(f"Error in send_data_to_mysql - \n{e}")


def predict_missing(data, conn) -> Union[str, str]:
    """Execute the prediction query

    Args:
        data (list): feature vlaues for ML model from IoT device
        conn (Connection): MindsDB MySQL connection object

    Returns:
        [str, str]: prediction_class string and prediction explanation in json string
    """
    data = list(map(float, data.split(",")))
    str_query = QUERY_PREDICTION.format(*data)

    missing, json_missing_explain = conn.execute(text(str_query)).fetchone()
    logging.info(f"Predicted class - {missing}")
    return missing, json_missing_explain

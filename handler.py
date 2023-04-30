import streamlit as st
from sqlalchemy import create_engine, text
import os
import sys
import logging
import coloredlogs
from constants import QUERY_PREDICTION


user = os.environ.get("MINDSDB_USERNAME")
password = os.environ.get("MINDSDB_PASSWORD")
host = "cloud.mindsdb.com"
port = 3306
database = "mindsdb"

log_handler = logging.StreamHandler(sys.stdout)
str_fmt = "%(asctime)s | %(module)s | line %(lineno)d | %(levelname)s | %(message)s"
formatter = logging.Formatter(str_fmt)
log_handler.setFormatter(formatter)

coloredlogs.install(level=logging.INFO, handlers=[log_handler], fmt=str_fmt)


@st.cache_resource
def create_connection():
    logging.info("Attempting to create a connection")
    return create_engine(
        url=f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    ).connect()


def generate_query(data):
    return QUERY_PREDICTION.format(*data)


def handle_send_data(data):
    try:
        with st.spinner("Querying mindsdb to make a prediction..."):
            conn = create_connection()
            logging.info("Successfully connected to mindsdb")
            str_query = generate_query(data)

            logging.info("Attempting to execute the query")
            missing, json_missing_explain = conn.execute(text(str_query)).fetchone()
            logging.info("Successfully executed the query")

        st.session_state.NO_OF_PREDICTION += 1

        st.session_state.FLAG_PREDICTION = True

        missing = "missing" if int(missing) == 1 else "not missing"

        st.session_state.PREDICTION = f"Predicted class - {missing}"

        st.session_state.JSON_PREDICTION = json_missing_explain

        # logging.debug(f"Query return - {missing} - {json_missing_explain}")
    except Exception as e:
        logging.exception("Error in handle_send_data")

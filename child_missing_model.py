import streamlit as st
import logging

from handler import handle_send_data

st.set_page_config(
    page_title="Sandy Inspires - Child Missing",
    page_icon="📈",
    layout="wide",
)

st.title("Dashboard - Child Missing")

# initializing values
if "FLAG_PREDICTION" not in st.session_state:
    logging.info("Initializing variables")
    st.session_state.NO_OF_PREDICTION = 0
    st.session_state.FLAG_PREDICTION = False


col1, col2 = st.columns([2, 2])

with col1:
    input_latitude = st.number_input(
        "Latitude",
        min_value=13.0,
        max_value=14.0,
        value=13.874,
        help="Latitude of the IoT device",
    )
    input_longitude = st.number_input(
        "Longitude",
        min_value=79.0,
        max_value=81.0,
        value=80.725,
        help="Longitude of the IoT device",
    )
    input_vibration = st.number_input(
        "Vibration (g)",
        value=3.9,
        help="Vibration of the IoT device",
    )
    input_acceleration = st.number_input(
        "Acceleration (m/s^2)",
        value=25.7,
        help="Acceleration of the IoT device",
    )

    st.button(
        "Send Data",
        on_click=handle_send_data,
        args=((input_latitude, input_longitude, input_vibration, input_acceleration),),
    )


with col2:
    # shows how many prediction is made in this session
    st.text(f"No of Predictions made - {st.session_state.NO_OF_PREDICTION}")

    if st.session_state.FLAG_PREDICTION:
        st.text("Prediction output")
        st.text(st.session_state.PREDICTION)
        st.json(st.session_state.JSON_PREDICTION)

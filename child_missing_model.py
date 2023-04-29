import streamlit as st

st.set_page_config(
    page_title="Sandy Inspires - Child Missing",
    page_icon="📈",
    layout="wide",
)

st.title("Dashboard - Child Missing")

col1, col2 = st.columns([3,1])

with col2:
    st.button("Refresh")
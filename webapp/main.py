import streamlit as st
import requests

st.set_page_config(page_title="Soccer Predictor", layout="wide")
st.title("⚽ Soccer Prediction Dashboard")

st.sidebar.success("Select a page above.")

st.write("### Welcome to the MLOps Soccer Prediction Platform")
st.write("This application interacts with the Model API and Airflow Ingestion jobs.")

# Health check test
if st.button("Check API Health"):
    try:
        response = requests.get("http://api:8000/health")
        st.write(f"API Status: {response.json()['status']}")
    except Exception as e:
        st.error(f"Could not connect to API: {e}")
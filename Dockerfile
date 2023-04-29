FROM python3.8.16-slim

RUN mkdir -p /opt/streamlit/child_missing_model

COPY . /opt/streamlit/child_missing_model

RUN pip install -r requirements.txt

EXPOSE 8501

CMD streamlit run child_missing_model.py
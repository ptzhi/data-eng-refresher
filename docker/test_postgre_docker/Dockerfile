FROM python:3.11

RUN pip install pandas pyarrow sqlalchemy psycopg2

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY ./ny_taxi_local_data ./ny_taxi_local_data

ENTRYPOINT ["python","ingest_data.py"]

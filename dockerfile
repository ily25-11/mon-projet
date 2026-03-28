FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y gcc

USER airflow

RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    selenium \
    fake-useragent \
    cloudscraper \
    psycopg2-binary
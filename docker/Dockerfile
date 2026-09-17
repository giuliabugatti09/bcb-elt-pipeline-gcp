FROM apache/airflow:2.10.2-python3.11

COPY requirements.txt /requirements.txt
COPY docker/requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    -r /requirements.txt \
    -r /requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.11.txt"
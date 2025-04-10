#!/bin/bash
set -e

if [ -f "/opt/airflow/requirements.txt" ]; then
    python3 -m pip install --upgrade pip
    python3 -m pip install --user -r /opt/airflow/requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

airflow db upgrade

exec airflow webserver
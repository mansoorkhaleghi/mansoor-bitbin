FROM apache/airflow:2.9.2-python3.12

# Step 1: run as root to install OS packages
USER root

RUN apt-get update && \
    apt-get install -y build-essential libpq-dev gcc python3-dev libssl-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Step 2: switch back to airflow user for pip
USER airflow

# Upgrade pip, setuptools, wheel
RUN pip install --upgrade pip setuptools wheel

# Copy requirements and install
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

FROM apache/airflow:latest

# Install build dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt .
COPY my_funcs /opt/airflow/my_funcs
COPY setup.py /opt/airflow/setup.py

RUN pip install /opt/airflow

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/opt/airflow/custom_functions:${PYTHONPATH}"

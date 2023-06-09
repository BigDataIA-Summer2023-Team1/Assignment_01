FROM apache/airflow:2.6.1
USER root
RUN apt-get update && apt-get -y upgrade
# RUN apt-get install -y redis-tools
RUN apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

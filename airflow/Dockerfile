FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN usermod -aG docker airflow

USER airflow

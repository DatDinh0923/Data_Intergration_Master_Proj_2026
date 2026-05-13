FROM apache/airflow:2.8.1
USER root

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

USER airflow

# Install PySpark
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    boto3 \
    s3fs
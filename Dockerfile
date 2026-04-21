# Start with the official Airflow image
FROM apache/airflow:2.8.1

# Switch to the root user to install system packages
USER root

# Install Java (Required for Apache Spark to run)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Switch back to the airflow user
USER airflow

# Install PySpark (This gives us the spark-submit command!)
RUN pip install --no-cache-dir pyspark==3.5.0
#!/bin/bash
# Captures docker stats every 2 seconds and saves to a CSV
OUTPUT_FILE="docker_stats.csv"

echo "Timestamp,Container,CPU_Percent,Memory_Usage" > $OUTPUT_FILE

echo "Monitoring started. Press [CTRL+C] to stop."
while true; do
  TIMESTAMP=$(date +"%H:%M:%S")
  # Filter for your Spark/Airflow worker containers
  docker compose stats --no-stream --format "$TIMESTAMP,{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | grep 'spark\|worker' >> $OUTPUT_FILE
  sleep 2
done
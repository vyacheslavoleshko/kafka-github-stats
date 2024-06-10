#!/bin/bash

# Variables
#GITHUB_FETCHER_DIR="github_fetcher/src/main/java"
#GITHUB_FETCHER_MAIN_CLASS="GithubFetcher"
#GITHUB_FETCHER_CLASS_OUTPUT_DIR="out"
#
#GITHUB_ANALYZER_DIR="github_stats/src/main/java"
#GITHUB_ANALYZER_MAIN_CLASS="GithubAnalyzer"
#GITHUB_ANALYZER_CLASS_OUTPUT_DIR="out"

# Compile and Run Java source code
#javac -d "$CLASS_OUTPUT_DIR" $(find "$SOURCE_DIR" -name "*.java")
#java -cp "$CLASS_OUTPUT_DIR" "$MAIN_CLASS"

# Run docker compose
docker-compose up -d

# Navigate to the specified directory
cd directory_reader/src/main/resources || exit

# Create directories if they do not exist
mkdir -p sourcedir
mkdir -p errors
mkdir -p finished

# Create Kafka topics
# Create github.accounts topic
kafka-topics.sh --create --topic github.accounts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create github.commits topic
kafka-topics.sh --create --topic github.commits --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create github.stats topic
kafka-topics.sh --create --topic github.stats --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create github.stats Consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic github.stats --group stats-consumers


echo "Setup complete."

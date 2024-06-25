#!/bin/bash

# Variables
YOUR_GITHUB_TOKEN="$@"

# Run docker compose
docker-compose up -d

sleep 5

# Create Kafka topics
# Create github.accounts topic
kafka-topics.sh --create --topic github.accounts --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --partitions 3 --replication-factor 3

# Create github.commits topic
kafka-topics.sh --create --topic github.commits --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --partitions 3 --replication-factor 3

# Create github.stats topic
kafka-topics.sh --create --topic github.stats --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --partitions 3 --replication-factor 3

# Run applications
#cd directory_reader
#mvn clean package
#java -jar target/kafka-github-spool-1.0-SNAPSHOT.jar

# Run GithubFetcher app to retrieve github commit data
cd github_fetcher
mvn clean package
java -jar target/kafka-github-fetcher-1.0-SNAPSHOT.jar $YOUR_GITHUB_TOKEN > github_fetcher1.log 2>&1 &
java -jar target/kafka-github-fetcher-1.0-SNAPSHOT.jar $YOUR_GITHUB_TOKEN > github_fetcher2.log 2>&1 &

# Run GithubAnalyzer app to calculate statistics for commits
cd ..
cd github_stats
mvn clean package
java -jar target/kafka-github-stats-1.0-SNAPSHOT.jar http://localhost 8070 github_analyzer1 > github_analyzer1.log 2>&1 &
sleep 5
java -jar target/kafka-github-stats-1.0-SNAPSHOT.jar http://localhost 8071 github_analyzer2 > github_analyzer2.log 2>&1 &

# Run Frontend
cd ..
cd web_ui
npm install
npm run serve > web_ui.log 2>&1 &

# Navigate to the specified directory
cd ..
cd directory_reader/src/main/resources || exit

# Create directories if they do not exist
mkdir -p sourcedir
mkdir -p errors
mkdir -p finished

echo "Setup complete."
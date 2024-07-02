#!/bin/bash

# Variables
YOUR_GITHUB_TOKEN="$@"
GITHUB_FETCHER_JAR_FILE="target/kafka-github-fetcher-1.0-SNAPSHOT.jar"
GITHUB_ANALYZER_JAR_FILE="target/kafka-github-stats-1.0-SNAPSHOT.jar"

# Run docker compose
docker-compose up -d

cd directory_reader

# Create directories if they do not exist
mkdir -p sourcedir
mkdir -p errors
mkdir -p finished

sleep 10

# Function to create Kafka topics
create_kafka_topic() {
    local container_name=$1
    local topic_name=$2
    local partitions=$3
    local replication_factor=$4
    local compact=$5

    echo "Creating Kafka topic: $topic_name for [$container_name]"

    if [ "$compact" = "true" ]; then
        docker exec $container_name kafka-topics \
            --create \
            --topic $topic_name \
            --partitions $partitions \
            --replication-factor $replication_factor \
            --bootstrap-server INTERNAL://broker-2:29092 \
            --config cleanup.policy=compact
    else
        docker exec $container_name kafka-topics \
            --create \
            --topic $topic_name \
            --partitions $partitions \
            --replication-factor $replication_factor \
            --bootstrap-server INTERNAL://broker-2:29092
    fi
}

# Create Kafka topics
# Create github.accounts topic
create_kafka_topic "broker-2" "github.accounts" 3 3 "false"
create_kafka_topic "broker-2" "github.commits" 3 3 "true"
create_kafka_topic "broker-2" "github.stats" 3 3 "true"
create_kafka_topic "broker-2" "connect-configs" 1 3 "true"
create_kafka_topic "broker-2" "connect-offsets" 50 3 "true"
create_kafka_topic "broker-2" "connect-status" 10 3 "true"

# Run applications
# Run GithubFetcher app to retrieve github commit data
cd ..
cd github_fetcher
mvn clean package -Dmaven.test.skip=true
# Check if Maven build was successful
if [ $? -eq 0 ]; then
  echo "Maven build successful. JAR file created."
else
  echo "Maven build failed. Check the Maven logs for details."
  exit 1
fi
java -jar $GITHUB_FETCHER_JAR_FILE $YOUR_GITHUB_TOKEN > github_fetcher1.log 2>&1 &
java -jar $GITHUB_FETCHER_JAR_FILE $YOUR_GITHUB_TOKEN > github_fetcher2.log 2>&1 &

# Run GithubAnalyzer app to calculate statistics for commits
cd ..
cd github_stats
mvn clean package -Dmaven.test.skip=true
# Check if Maven build was successful
if [ $? -eq 0 ]; then
  echo "Maven build successful. JAR file created."
else
  echo "Maven build failed. Check the Maven logs for details."
  exit 1
fi
java -jar $GITHUB_ANALYZER_JAR_FILE http://localhost 8070 github_analyzer /tmp/kafka-streams/github_analyzer1 > github_analyzer1.log 2>&1 &
sleep 5
java -jar $GITHUB_ANALYZER_JAR_FILE http://localhost 8071 github_analyzer /tmp/kafka-streams/github_analyzer2 > github_analyzer2.log 2>&1 &

# Run Frontend
cd ..
cd web_ui
npm install
npm run serve > web_ui.log 2>&1 &

# Post Connector configuration
while true; do
  response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)
  if [ "$response" -eq 200 ]; then
    break
  fi
  echo "Kafka Connect is not available yet (HTTP status: $response). Waiting..."
  sleep 5
done
echo "Kafka Connect is available."

echo "Submitting connector configuration..."
cd ..
cd directory_reader
curl -X POST -H "Content-Type: application/json" --data @spooldir-connector.json http://localhost:8083/connectors

echo "Setup complete."

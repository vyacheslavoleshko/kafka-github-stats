#!/bin/bash

# Variables
YOUR_GITHUB_TOKEN="$@"

#cd directory_reader
#mvn clean package
#java -jar target/kafka-github-spool-1.0-SNAPSHOT.jar

cd ..
cd github_fetcher
mvn clean package
java -jar target/kafka-github-fetcher-1.0-SNAPSHOT.jar $YOUR_GITHUB_TOKEN

cd ..
cd github_stats
mvn clean package
java -jar target/kafka-github-stats-1.0-SNAPSHOT.jar
#
## Run docker compose
#cd ..
#docker-compose up -d
#
## Navigate to the specified directory
#cd directory_reader/src/main/resources || exit
#
## Create directories if they do not exist
#mkdir -p sourcedir
#mkdir -p errors
#mkdir -p finished
#
## Create Kafka topics
## Create github.accounts topic
#kafka-topics.sh --create --topic github.accounts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
## Create github.commits topic
#kafka-topics.sh --create --topic github.commits --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
## Create github.stats topic
#kafka-topics.sh --create --topic github.stats --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#
## Create github.stats Consumer
#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic github.stats --group stats-consumers


echo "Setup complete."

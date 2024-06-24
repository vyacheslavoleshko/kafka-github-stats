#!/bin/bash

# Specify the paths to your Java applications
app_paths=(
    "target/kafka-github-fetcher-1.0-SNAPSHOT.jar"
    "target/kafka-github-stats-1.0-SNAPSHOT.jar"
)

# Loop through each application path and stop the corresponding Java process
for app_path in "${app_paths[@]}"; do
    # Find the PID of the Java process running the application
    pid=$(ps aux | grep "[j]ava -jar $app_path" | awk '{print $2}')

    # If a PID was found, kill the process
    if [ -n "$pid" ]; then
        echo "Stopping Java application running: $app_path (PID: $pid)"
        kill "$pid"
        echo "Application stopped."
    else
        echo "No running Java application found for: $app_path"
    fi
done

docker-compose down
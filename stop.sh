#!/bin/bash

# Specify the paths to your Java applications
app_paths=(
    "target/kafka-github-fetcher-1.0-SNAPSHOT.jar"
    "target/kafka-github-stats-1.0-SNAPSHOT.jar"
)

# Loop through each application path and stop the corresponding Java process
for app_path in "${app_paths[@]}"; do

    # Find the PIDs of the Java processes running the application
    pids=$(ps aux | grep "[j]ava -jar $app_path" | awk '{print $2}')

    # Debugging output
    echo "PIDs found: $pids"

    # If any PIDs were found, send SIGTERM to each process
    if [ -n "$pids" ]; then
        for pid in $pids; do
            echo "Stopping Java application running: $app_path (PID: $pid)"
            kill "$pid"
            echo "SIGTERM sent to PID $pid. Waiting for graceful shutdown..."
        done
    else
        echo "No running Java application found for: $app_path"
    fi
done

echo "Stopping UI..."
node_pid=$(ps aux | grep '[n]pm run serve' | awk '{print $2}')

# Debugging output
echo "Node.js PID found: $node_pid"

# If a PID was found, send SIGTERM to the process
if [ -n "$node_pid" ]; then
    echo "Stopping Node.js application (PID: $node_pid)"
    kill "$node_pid"
    echo "Node.js application shutdowns gracefully."
else
    echo "No running Node.js application found."
fi

docker-compose down
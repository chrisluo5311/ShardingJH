#!/bin/bash
set -e

# Accept profile from argument (e.g., ./run-app.sh server1)
PROFILE=$1

if [ -z "$PROFILE" ]; then
  echo "Usage: ./run-app.sh <profile> (e.g., server1, server2, server3)"
  exit 1
fi

echo "Starting deployment for profile: $PROFILE"

# Stop any running Spring Boot processes (optional safety)
echo "Stopping existing Spring Boot app if running..."
pkill -f 'java.*\.jar' || true

# Build the project and package it
echo "Building Spring Boot project..."
./mvnw clean package -DskipTests

# Find the generated JAR
JAR_FILE=$(ls target/*.jar | head -n 1)
echo "Found JAR: $JAR_FILE"

# Launch with the correct profile
echo "Running JAR with --spring.profiles.active=$PROFILE"
# 2>&1 means “redirect error output to the same place as normal output”
# & : Runs the whole command in the background (non-blocking)
nohup java -jar "$JAR_FILE" --spring.profiles.active=$PROFILE > app.log 2>&1 &

echo "✅ App started with profile: $PROFILE"
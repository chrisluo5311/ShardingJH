#!/bin/bash
set -e

# Stop any existing Spring Boot processes (optional safety)
echo "Stopping existing Spring Boot app if running..."
pkill -f 'java.*\.jar' || true

# Build the project and package it as a runnable JAR (skip tests if desired)
echo "Building Spring Boot project..."
./mvnw clean package -DskipTests

# Find the generated JAR in the target directory
JAR_FILE=$(ls target/*.jar | head -n 1)
echo "Found JAR: $JAR_FILE"

# Run the application in the background with nohup
echo "Starting application in background..."
nohup java -jar "$JAR_FILE" > app.log 2>&1 &

echo "App launched in background. Logs written to app.log"
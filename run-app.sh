#!/bin/bash
set -e

# Build the project and package it as a runnable JAR (skip tests if desired)
./mvnw clean package -DskipTests

# Find the generated JAR in the target directory
JAR_FILE=$(ls target/*.jar | head -n 1)

# Run the application
java -jar "$JAR_FILE"
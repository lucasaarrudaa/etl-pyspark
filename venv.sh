#!/bin/bash

# Path to the root of the project
PROJETO_DIR="$(pwd)/etl-spark"

# Build the Docker image using the Dockerfile
echo "Building the Docker image..."
docker build -t meu_projeto $PROJETO_DIR/Docker

# Run docker-compose
echo "Running docker-compose..."
docker-compose -f $PROJETO_DIR/Docker/docker-compose.yaml up -d

# Create Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv $PROJETO_DIR/venv

# Activate the Python virtual environment
echo "Activating Python virtual environment..."
source $PROJETO_DIR/venv/bin/activate

# Install project dependencies using requirements.txt
echo "Installing project dependencies..."
pip install -r $PROJETO_DIR/requirements.txt

# Open VSCode in the context of the Docker container
echo "Opening VSCode in the context of the Docker container..."
docker exec -it meu_projeto code /app

echo "Concluded."

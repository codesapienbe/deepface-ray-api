#! /bin/bash

# Stop and remove the container if it exists
docker rm -f deepface-ray-api || true

# Build the Docker image
docker build -t deepface-ray-api .

# Run the Docker container  
docker run --shm-size=2g --rm -p 8000:8000 --name deepface-ray-api deepface-ray-api


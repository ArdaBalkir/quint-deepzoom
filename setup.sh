#!/bin/bash
# Create required directories for the distributed DeepZoom service

# Create directories
mkdir -p ./app/static
mkdir -p ./kubernetes

# Ensure dashboard.html is in the static directory
if [ ! -f ./app/static/dashboard.html ]; then
    if [ -f ./dashboard.html ]; then
        cp ./dashboard.html ./app/static/
    else
        echo "Warning: dashboard.html not found!"
    fi
fi

# Create directories in shared volume for the first time
echo "Creating data directories..."
mkdir -p /data/downloads /data/outputs
chmod -R 777 /data

echo "Setup complete!"

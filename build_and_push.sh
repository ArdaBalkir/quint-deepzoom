#!/bin/bash
# Script to build and push Docker images for DeepZoom service

# Set the registry path
REGISTRY=${REGISTRY:-"docker-registry.ebrains.eu/workbench"}

# Build API image
echo "Building API image..."
docker build -t $REGISTRY/deepzoom-api:latest -f api.Dockerfile .

# Build Downloader image
echo "Building Downloader image..."
docker build -t $REGISTRY/deepzoom-downloader:latest -f downloader.Dockerfile .

# Build Processor image
echo "Building Processor image..."
docker build -t $REGISTRY/deepzoom-processor:latest -f processor.Dockerfile .

# Build Uploader image
echo "Building Uploader image..."
docker build -t $REGISTRY/deepzoom-uploader:latest -f uploader.Dockerfile .

# Push images to registry
echo "Pushing images to registry..."
docker push $REGISTRY/deepzoom-api:latest
docker push $REGISTRY/deepzoom-downloader:latest
docker push $REGISTRY/deepzoom-processor:latest
docker push $REGISTRY/deepzoom-uploader:latest

echo "All images built and pushed successfully!"

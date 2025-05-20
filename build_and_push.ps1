# PowerShell script to build and push Docker images for DeepZoom service

# Check if REGISTRY environment variable is set
if (-not $env:REGISTRY) {
    $env:REGISTRY = "your-registry.com/deepzoom"
    Write-Host "REGISTRY environment variable not set. Using default: $env:REGISTRY"
}

# Build API image
Write-Host "Building API image..."
docker build -t "$env:REGISTRY/deepzoom-api:latest" -f api.Dockerfile .

# Build Downloader image
Write-Host "Building Downloader image..."
docker build -t "$env:REGISTRY/deepzoom-downloader:latest" -f downloader.Dockerfile .

# Build Processor image
Write-Host "Building Processor image..."
docker build -t "$env:REGISTRY/deepzoom-processor:latest" -f processor.Dockerfile .

# Build Uploader image
Write-Host "Building Uploader image..."
docker build -t "$env:REGISTRY/deepzoom-uploader:latest" -f uploader.Dockerfile .

# Push images to registry
Write-Host "Pushing images to registry..."
docker push "$env:REGISTRY/deepzoom-api:latest"
docker push "$env:REGISTRY/deepzoom-downloader:latest"
docker push "$env:REGISTRY/deepzoom-processor:latest"
docker push "$env:REGISTRY/deepzoom-uploader:latest"

Write-Host "All images built and pushed successfully!"

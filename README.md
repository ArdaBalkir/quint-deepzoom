# CreateZoom API - Distributed Architecture

## Overview

This application is an asynchronous service for creating DeepZoom image file formats for EBrains software. It has been refactored to run in a distributed architecture with multiple containers, supporting horizontal scaling in a Kubernetes environment.

## Architecture

The application now uses the following components:

1. **Redis Queue** - A Redis instance acting as a message broker between different services
2. **API Service** - Handles HTTP requests, enqueues tasks, and provides status information
3. **Downloader Worker** - Downloads files from the source location (horizontally scalable)
4. **Processor Worker** - Creates DeepZoom pyramids and compresses them (horizontally scalable)
5. **Uploader Worker** - Uploads the final compressed files to the target location

### Workflow

1. Client sends a request to the API with `path`, `target_path`, and `token`
2. API creates a task in Redis and enqueues it in the download queue
3. Downloader worker picks up the task, downloads the file, and enqueues it in the processing queue
4. Processor worker creates the DeepZoom pyramid, compresses it, and enqueues it in the upload queue
5. Uploader worker uploads the file and completes the task

## API Endpoints

- `GET /deepzoom/health` - Service health status
- `POST /deepzoom` - Submit image processing task
- `GET /deepzoom/status/{task_id}` - Check task status
- `GET /deepzoom/tasks` - Admin endpoint to view all tasks (requires authentication)

## Task Request Format

```json
{
  "path": "str",
  "target_path": "str",
  "token": "str"
}
```

## Deployment Options

### Docker Compose (Development)

For local development or testing, you can use Docker Compose:

```bash
# Start all services
docker-compose up -d

# Scale specific workers
docker-compose up -d --scale downloader=3 --scale processor=2
```

### Kubernetes (Production)

For production deployment in Rancher or other Kubernetes environments:

```bash
# Update the registry in the deployment.yaml file
$registry = "your-registry.com/deepzoom"
(Get-Content -Path kubernetes/deployment.yaml) -replace '\$\{REGISTRY\}', $registry | Set-Content -Path kubernetes/deployment.yaml

# Apply the configuration
kubectl apply -f kubernetes/deployment.yaml
```

#### Building and Pushing Docker Images

```bash
# Set your registry path
$env:REGISTRY = "your-registry.com/deepzoom"

# On Windows PowerShell
./build_and_push.ps1

# Or on Linux/macOS
./build_and_push.sh
```

## Scaling

The Kubernetes deployment includes HorizontalPodAutoscalers (HPAs) for the downloader and processor workers:

- Downloader workers scale from 1 to 5 based on CPU utilization (70%)
- Processor workers scale from 1 to 3 based on CPU utilization (70%)

This architecture allows for efficient resource utilization and high throughput by processing multiple tasks in parallel.

## Development Setup

For local development, you can run a Redis container:

```bash
docker run -d -p 6379:6379 --name redis redis:6.2-alpine
```

Then run each component separately:

```powershell
# Set environment variables
$env:REDIS_HOST = "localhost"
$env:REDIS_PORT = "6379"

# API
uvicorn app.main_redis:app --reload --host 0.0.0.0 --port 8000

# Downloader Worker
python -m app.downloader_worker

# Processor Worker
python -m app.processor_worker

# Uploader Worker
python -m app.uploader_worker
```

## Dependencies

Main dependencies:
- Redis (message broker)
- FastAPI (API framework)
- Pyvips (image processing)
- Aiohttp, Aiofiles, Asyncio (async operations)

Files paths should be submitted individually, for each file you will be assigned a `task_id`. You can query your tasks status with the status endpoint.

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant TaskManager
    participant TaskStore
    participant Storage
    participant Bucket

    Client->>API: POST /deepzoom
    API->>TaskManager: Create new task
    TaskManager->>TaskStore: Store task details
    API-->>Client: Return task_id

    Note over TaskManager: Async Processing
    Bucket->>TaskManager: Download image
    TaskManager->>Storage: Create DeepZoomImage
    TaskManager->>Storage: Zip store
    TaskManager->>Bucket: Upload result
    TaskManager->>TaskStore: Update status

    Client->>API: GET /deepzoom/status/{task_id}
    API->>TaskStore: Get task status
    API-->>Client: Return task details

```

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import aiohttp
import asyncio
import uuid
from datetime import datetime
import logging
import sys
import pathlib

# Import Redis task store
from .redis_store import RedisTaskStore, enqueue_task, DOWNLOAD_QUEUE

# Constants
DATA_ROOT = "/data"
DOWNLOADS_DIR = os.path.join(DATA_ROOT, "downloads")
OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")

# Update the logging configuration to use /data directory
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis task store
task_store = RedisTaskStore()


@app.on_event("startup")
async def startup_event():
    # Ensure Redis connection is established
    await task_store.get_redis()
    logger.info("API service started - connected to Redis")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/deepzoom/health")
async def health():
    return {"status": "I'm alive!"}


@app.post("/deepzoom", status_code=202)
async def deepzoom_endpoint(request: Request):
    try:
        data = await request.json()
        logger.info("Received deepzoom request")

        for param in ["path", "target_path", "token"]:
            if not data.get(param):
                logger.error(f"Missing parameter: {param}")
                raise HTTPException(
                    status_code=400, detail=f"Missing required parameter: {param}"
                )
            if not isinstance(data[param], str) or not data[param].strip():
                logger.error(f"Invalid parameter: {param}")
                raise HTTPException(
                    status_code=400, detail=f"{param} must be a non-empty string"
                )

        task_id = str(uuid.uuid4())
        logger.info(f"Creating task {task_id}")

        # Add task to Redis store
        await task_store.add_task(
            task_id, {"path": data["path"], "target_path": data["target_path"]}
        )

        # Queue the task for download
        await enqueue_task(
            DOWNLOAD_QUEUE,
            {
                "task_id": task_id,
                "path": data["path"],
                "target_path": data["target_path"],
                "token": data["token"],
            },
        )

        # Update task status to queued
        await task_store.update_task(
            task_id, {"status": "queued", "current_step": "queued_for_download"}
        )

        response = {
            "task_id": task_id,
            "status": "accepted",
            "status_endpoint": f"/deepzoom/status/{task_id}",
        }
        return response

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/deepzoom/status/{task_id}")
async def get_task_status(task_id: str):
    try:
        logger.debug(f"Checking status for task: {task_id}")
        task = await task_store.get_task(task_id)
        if not task:
            logger.warning(f"Task not found: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")
        return task
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}", exc_info=True)
        raise


# Currently to provide internal access only
async def verify_ebrains_token(token: str) -> bool:
    """Verify token and check if email ends with @medisin.uio.no"""
    url = "https://iam.ebrains.eu/auth/realms/hbp/protocol/openid-connect/userinfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url, headers={"Authorization": f"Bearer {token}"}
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("email", "").endswith("@medisin.uio.no")
    return False


@app.get("/deepzoom/tasks")
async def get_all_tasks(request: Request):
    try:
        # Get token from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid token")

        token = auth_header.split(" ")[1]
        is_authorized = await verify_ebrains_token(token)
        if not is_authorized:
            raise HTTPException(
                status_code=403, detail="Unauthorized email domain"
            )  # Get all tasks from Redis
        tasks = await task_store.get_all_tasks()
        await task_store.cleanup_old_tasks()

        return {
            "tasks": tasks,
            "total": len(tasks),
        }

    except Exception as e:
        logger.error(f"Error getting tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboard")
async def get_dashboard():
    """Serve the monitoring dashboard"""
    dashboard_path = os.path.join(
        pathlib.Path(__file__).parent.absolute(), "static", "dashboard.html"
    )
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    else:
        raise HTTPException(status_code=404, detail="Dashboard not found")
        raise HTTPException(status_code=500, detail=str(e))

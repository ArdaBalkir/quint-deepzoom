from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pyvips
import os
import zipfile
import aiohttp
import aiofiles
import asyncio
from aiofiles.os import makedirs, remove, path
import shutil
from datetime import datetime, timedelta
import uuid
from typing import Dict, Optional
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("deepzoom.log")],
)
logger = logging.getLogger(__name__)

# Constants
CHUNK_SIZE = 16 * 1024 * 1024
# To increase download stream speed


app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# TODO Threaded test for performance
# TODO Add origins
# TODO Add token and user validation


class TaskStore:
    """
    Manages tasks and their statuses
    Default TTL is 24 hours
    """

    def __init__(self, ttl_hours=24):
        self.tasks: Dict[str, dict] = {}
        self.ttl = timedelta(hours=ttl_hours)
        logger.info("TaskStore initialized with TTL: %d hours", ttl_hours)

    def add_task(self, task_id: str, task_data: dict):
        self.tasks[task_id] = {
            **task_data,
            "created_at": datetime.now(),
            "status": "pending",
            "current_step": "initialized",
            "step_details": None,
            "result": None,
            "error": None,
            "progress": 0,
        }
        logger.info(f"Added new task: {task_id}")

    def update_task(self, task_id: str, updates: dict):
        if task_id in self.tasks:
            self.tasks[task_id].update(updates)
            logger.debug(f"Updated task {task_id}: {updates}")

    def get_task(self, task_id: str) -> Optional[dict]:
        return self.tasks.get(task_id)

    def cleanup_old_tasks(self):
        before_count = len(self.tasks)
        now = datetime.now()
        self.tasks = {
            task_id: task
            for task_id, task in self.tasks.items()
            if now - task["created_at"] < self.ttl
        }
        after_count = len(self.tasks)
        logger.info(f"Cleaned up {before_count - after_count} old tasks")


class TaskManager:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(6)
        self.task_store = TaskStore()
        logger.info("TaskManager initialized")

    async def add_task(self, task_id: str, path: str, target_path: str, token: str):
        self.task_store.add_task(task_id, {"path": path, "target_path": target_path})
        asyncio.create_task(self._process_task(task_id, path, target_path, token))
        return task_id

    async def _process_task(
        self, task_id: str, path: str, target_path: str, token: str
    ):
        async with self.semaphore:
            try:
                logger.info(f"Starting task {task_id} processing")
                # Download
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "processing",
                        "current_step": "downloading",
                        "progress": 0,
                    },
                )
                download_path = await get_download(
                    path, token, task_id, self.task_store
                )

                # DeepZoom
                self.task_store.update_task(
                    task_id, {"current_step": "creating_deepzoom", "progress": 25}
                )
                dzi_path = await deepzoom(download_path)

                # Zip
                self.task_store.update_task(
                    task_id, {"current_step": "compressing", "progress": 50}
                )
                zip_path = await zip_pyramid(dzi_path)

                # Upload
                self.task_store.update_task(
                    task_id, {"current_step": "uploading", "progress": 75}
                )
                upload_filename = os.path.basename(zip_path)
                result = await upload_zip(
                    f"{target_path}/{upload_filename}", zip_path, token
                )

                # Success
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "completed",
                        "current_step": "completed",
                        "result": result,
                        "progress": 100,
                    },
                )
                logger.info(f"Task {task_id} completed successfully")

                # Cleanup
                await cleanup_files(
                    os.path.join("temp/downloads", os.path.basename(path)),
                    dzi_path,
                    zip_path,
                )

            except Exception as e:
                logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "failed",
                        "current_step": "failed",
                        "error": str(e),
                        "progress": 0,
                    },
                )
                await cleanup_files(
                    os.path.join("temp/downloads", os.path.basename(path)),
                    dzi_path if "dzi_path" in locals() else None,
                    zip_path if "zip_path" in locals() else None,
                )


# Initialize global task manager
task_manager = TaskManager()


async def get_download(path: str, token: str, task_id: str, task_store: TaskStore):
    """
    Asynchronous definition for downloading file from EBrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                download_url = data.get("url")
                if not download_url:
                    raise Exception("Download URL not provided in response")

                await makedirs("temp/downloads", exist_ok=True)
                filename = os.path.basename(path)
                filepath = os.path.join("temp/downloads", filename)

                async with session.get(download_url) as download_response:
                    if download_response.status == 200:
                        total_size = int(
                            download_response.headers.get("content-length", 0)
                        )
                        downloaded_size = 0

                        async with aiofiles.open(filepath, "wb") as file:
                            async for chunk in download_response.content.iter_chunked(
                                CHUNK_SIZE
                            ):
                                await file.write(chunk)
                                downloaded_size += len(chunk)
                                progress = (
                                    int((downloaded_size / total_size) * 25)
                                    if total_size
                                    else 0
                                )
                                task_store.update_task(task_id, {"progress": progress})

                        logger.info(f"Download completed for task {task_id}")
                        return os.path.relpath(filepath)
                    else:
                        raise Exception(
                            f"Failed to download file. Status code: {download_response.status}"
                        )
            else:
                raise Exception(
                    f"Failed to get download URL. Status code: {response.status}"
                )


async def deepzoom(path: str):
    """
    Creates a DeepZoom pyramid from the image file specified
    Runs in a thread pool since pyvips operations are CPU-bound
    """

    def process_image():
        image = pyvips.Image.new_from_file(path)
        output_dir = os.path.abspath(os.path.join("temp", "outputs"))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, os.path.basename(path))
        image.dzsave(output_path)
        return output_path + ".dzi"

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(process_image)


async def upload_zip(upload_path: str, zip_path: str, token: str):
    """
    Asynchronous definition for uploading zip file to EBrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{upload_path}"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        # Get the upload URL
        async with session.put(url, headers=headers) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status, detail="Failed to get upload URL"
                )
            data = await response.json()
            upload_url = data.get("url")
            if not upload_url:
                raise HTTPException(
                    status_code=400, detail="Upload URL not provided in response"
                )

            print(f"Uploading to {upload_url}")
            async with aiofiles.open(zip_path, "rb") as file:
                file_data = await file.read()
                async with session.put(upload_url, data=file_data) as upload_response:
                    if upload_response.status == 201:
                        print(f"Created in {upload_path}")
                        return f"Created in {upload_path}"
                    else:
                        raise HTTPException(
                            status_code=upload_response.status,
                            detail="Failed to upload file",
                        )


async def zip_pyramid(path: str):
    """
    Function zips the pyramid files with a .dzip extension
    Runs in a thread pool since compression is CPU-bound
    """

    def create_zip():
        dzi_file = path
        dzi_dir = os.path.splitext(dzi_file)[0] + "_files"
        strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])
        zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(dzi_file, os.path.basename(dzi_file))
            for root, _, files in os.walk(dzi_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                    zipf.write(file_path, arcname)
        return zip_path

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(create_zip)


async def cleanup_files(download_path: str, dzi_path: str, zip_path: str):
    """
    Asynchronously remove temporary files after processing
    """
    try:
        if await path.exists(download_path):
            await remove(download_path)

        if dzi_path and await path.exists(dzi_path):
            await remove(dzi_path)
            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if await path.exists(dzi_dir):
                # Run rmtree in a thread pool since it's filesystem intensive
                await asyncio.to_thread(shutil.rmtree, dzi_dir)

        if zip_path and await path.exists(zip_path):
            await remove(zip_path)

    except Exception as e:
        print(f"Cleanup error: {str(e)}")


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

        await task_manager.add_task(
            task_id, data["path"], data["target_path"], data["token"]
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
        task = task_manager.task_store.get_task(task_id)
        if not task:
            logger.warning(f"Task not found: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")
        return task
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}", exc_info=True)
        raise

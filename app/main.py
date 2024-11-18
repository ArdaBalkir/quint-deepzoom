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

    def add_task(self, task_id: str, task_data: dict):
        """
        Adds a new task to the store
        Initial current_step is "initialized" which should be updated
        """
        self.tasks[task_id] = {
            **task_data,
            "created_at": datetime.now(),
            "status": "pending",
            "current_step": "initialized",
            "step_details": None,
            "result": None,
            "error": None,
        }

    def update_task(self, task_id: str, updates: dict):
        if task_id in self.tasks:
            self.tasks[task_id].update(updates)

    def get_task(self, task_id: str) -> Optional[dict]:
        return self.tasks.get(task_id)

    def cleanup_old_tasks(self):
        now = datetime.now()
        self.tasks = {
            task_id: task
            for task_id, task in self.tasks.items()
            if now - task["created_at"] < self.ttl
        }


class TaskManager:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(6)
        self.task_store = TaskStore()

    async def add_task(self, task_id: str, path: str, target_path: str, token: str):
        self.task_store.add_task(task_id, {"path": path, "target_path": target_path})
        asyncio.create_task(self._process_task(task_id, path, target_path, token))
        return task_id

    async def _process_task(
        self, task_id: str, path: str, target_path: str, token: str
    ):
        async with self.semaphore:
            try:
                # Download
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "processing",
                        "current_step": "downloading",
                    },
                )
                download_path = await get_download(path, token)

                # DeepZoom
                self.task_store.update_task(
                    task_id, {"current_step": "creating_deepzoom"}
                )
                dzi_path = await deepzoom(download_path)

                # Zip
                self.task_store.update_task(task_id, {"current_step": "compressing"})
                zip_path = await zip_pyramid(dzi_path)

                # Upload
                self.task_store.update_task(task_id, {"current_step": "uploading"})
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
                    },
                )

                # Cleanup
                await cleanup_files(
                    os.path.join("temp/downloads", os.path.basename(path)),
                    dzi_path,
                    zip_path,
                )

            # TODO Add explicit response to users depending on the error
            except Exception as e:
                self.task_store.update_task(
                    task_id,
                    {"status": "failed", "current_step": "failed", "error": str(e)},
                )
                await cleanup_files(
                    os.path.join("temp/downloads", os.path.basename(path)),
                    dzi_path if "dzi_path" in locals() else None,
                    zip_path if "zip_path" in locals() else None,
                )


# Initialize global task manager
task_manager = TaskManager()


async def get_download(path: str, token: str):
    """
    Asynchronous definition for downloading the file from Ebrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        # Get the download URL
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                download_url = data.get("url")
                if not download_url:
                    raise Exception("Download URL not provided in response")

                await makedirs("temp/downloads", exist_ok=True)
                filename = os.path.basename(path)
                filepath = os.path.join("temp/downloads", filename)

                # Stream the download asynchronously
                async with session.get(download_url) as download_response:
                    if download_response.status == 200:
                        async with aiofiles.open(filepath, "wb") as file:
                            async for chunk in download_response.content.iter_chunked(
                                8192
                            ):
                                await file.write(chunk)
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
    data = await request.json()

    # Parameter validation
    for param in ["path", "target_path", "token"]:
        if not data.get(param):
            raise HTTPException(
                status_code=400, detail=f"Missing required parameter: {param}"
            )
        if not isinstance(data[param], str) or not data[param].strip():
            raise HTTPException(
                status_code=400, detail=f"{param} must be a non-empty string"
            )

    task_id = str(uuid.uuid4())
    await task_manager.add_task(
        task_id, data["path"], data["target_path"], data["token"]
    )

    return {
        "task_id": task_id,
        "status": "accepted",
        "status_endpoint": f"/deepzoom/status/{task_id}",
    }


@app.get("/deepzoom/status/{task_id}")
async def get_task_status(task_id: str):
    task = task_manager.task_store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

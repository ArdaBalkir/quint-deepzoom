from fastapi import FastAPI, Request, HTTPException
from celery import chain
from app.tasks.io_tasks import download_image, zip_and_upload
from app.tasks.processing_tasks import process_image
from app.utils.db import init_db, get_task_status, update_task_status
import uuid

app = FastAPI()

init_db()


@app.post("/deepzoom", status_code=202)
async def deepzoom_endpoint(request: Request):
    """Trigger a DeepZoom processing chain and store initial task status."""
    data = await request.json()

    if "path" not in data or "token" not in data or "target_path" not in data:
        raise HTTPException(status_code=400, detail="Missing required fields")

    task_id = str(uuid.uuid4())

    # Initialize task status in the database
    update_task_status(task_id, "PENDING", progress=0)

    # Chain tasks: download -> process -> zip/upload
    task_chain = chain(
        download_image.s(data["path"], data["token"], task_id).set(queue="io_tasks"),
        process_image.s().set(queue="processing_tasks"),
        zip_and_upload.s(data["target_path"], data["token"]).set(queue="io_tasks"),
    )
    task_chain.apply_async(task_id=task_id)

    return {"task_id": task_id, "status_endpoint": f"/deepzoom/status/{task_id}"}


@app.get("/deepzoom/status/{task_id}")
async def get_task_status_endpoint(task_id: str):
    """Retrieve task status from the database."""
    status = get_task_status(task_id)
    if status:
        return status
    else:
        raise HTTPException(status_code=404, detail="Task not found")

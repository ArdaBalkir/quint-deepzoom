from celery import shared_task
from app.utils.deepzoom import deepzoom
from app.utils.db import update_task_status


@shared_task
def process_image(download_path, task_id):
    """Process an image into a DeepZoom pyramid and update task status."""
    update_task_status(task_id, "PROGRESS", progress=25)
    dzi_path = deepzoom(download_path)
    update_task_status(task_id, "PROGRESS", progress=75)
    return dzi_path

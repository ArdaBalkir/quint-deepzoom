from celery import shared_task
from app.utils.download import get_download
from app.utils.upload import upload_zip
from app.utils.zip import zip_pyramid
from app.utils.db import update_task_status


@shared_task
def download_image(path, token, task_id):
    """Download an image and update task status in the database."""
    update_task_status(task_id, "PROGRESS", progress=0)
    download_path = get_download(path, token, task_id)
    update_task_status(task_id, "PROGRESS", progress=25)
    # Add granular process back maybe
    return download_path


@shared_task
def zip_and_upload(dzi_path, target_path, token, task_id):
    """Zip the DeepZoom pyramid, upload it, and update task status."""
    update_task_status(task_id, "PROGRESS", progress=75)
    zip_path = zip_pyramid(dzi_path)
    update_task_status(task_id, "PROGRESS", progress=90)
    result = upload_zip(target_path, zip_path, token)
    update_task_status(task_id, "SUCCESS", progress=100, result=result)
    return result

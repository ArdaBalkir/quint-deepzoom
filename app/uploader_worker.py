import os
import asyncio
import json
import redis.asyncio as redis
import logging
import sys
import aiohttp
import aiofiles
import shutil
from datetime import datetime

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
UPLOAD_QUEUE = "upload_queue"  # Queue for upload tasks

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Global Redis connection pool
redis_pool = None


async def get_redis_connection():
    global redis_pool
    if redis_pool is None:
        redis_pool = redis.ConnectionPool.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
        )
    return redis.Redis(connection_pool=redis_pool)


async def update_task_status(task_id: str, updates: dict):
    r = await get_redis_connection()
    if await r.exists(f"task:{task_id}"):
        # Ensure all values are strings for hmset, especially complex ones like step_details
        str_updates = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in updates.items()
        }
        str_updates["updated_at"] = datetime.now().isoformat()
        await r.hmset(f"task:{task_id}", str_updates)
        logger.debug(f"Updated task {task_id} in Redis: {str_updates}")
    else:
        logger.warning(f"Attempted to update non-existent task {task_id} in Redis")


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
                raise Exception(
                    f"Failed to get upload URL. Status code: {response.status}"
                )
            data = await response.json()
            upload_url = data.get("url")
            if not upload_url:
                raise Exception("Upload URL not provided in response")

            logger.info(f"Uploading to {upload_url}")
            async with aiofiles.open(zip_path, "rb") as file:
                file_data = await file.read()
                async with session.put(upload_url, data=file_data) as upload_response:
                    if upload_response.status == 201 or upload_response.status == 200:
                        logger.info(
                            f"Upload successful to {upload_path} with status {upload_response.status}"
                        )
                        return f"Uploaded to {upload_path}"
                    else:
                        raise Exception(
                            f"Failed to upload file. Status code: {upload_response.status}"
                        )


async def cleanup_files(
    dzi_path: str = None, zip_path: str = None, download_path: str = None
):
    """
    Asynchronously remove temporary files after processing
    """
    try:
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
            logger.info(f"Removed downloaded file: {download_path}")

        if dzi_path and os.path.exists(dzi_path):
            os.remove(dzi_path)
            logger.info(f"Removed DZI file: {dzi_path}")

            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if os.path.exists(dzi_dir):
                shutil.rmtree(dzi_dir)
                logger.info(f"Removed DZI directory: {dzi_dir}")

        if zip_path and os.path.exists(zip_path):
            os.remove(zip_path)
            logger.info(f"Removed ZIP file: {zip_path}")

    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")


async def worker():
    logger.info(f"Uploader worker started. Listening to queue: {UPLOAD_QUEUE}")
    r = await get_redis_connection()

    while True:
        try:
            # Blocking pop from the upload queue
            message = await r.brpop(UPLOAD_QUEUE, timeout=0)
            if message:
                _, task_message_str = message
                task_data = json.loads(task_message_str)
                task_id = task_data.get("task_id")
                zip_path = task_data.get("zip_path")
                target_path = task_data.get("target_path")
                token = task_data.get("token")

                if not task_id or not zip_path or not target_path or not token:
                    logger.error(
                        f"Received task with missing required data: {task_data}"
                    )
                    continue

                logger.info(
                    f"Uploading task {task_id} - zip file: {zip_path} to {target_path}"
                )

                # 1. Update status to "uploading"
                await update_task_status(
                    task_id,
                    {
                        "status": "processing",
                        "current_step": "uploading",
                        "step_details": {
                            "message": "Uploading compressed DeepZoom files"
                        },
                        "progress": "75",
                    },
                )

                try:
                    # 2. Upload the ZIP file
                    upload_filename = os.path.basename(zip_path)
                    result = await upload_zip(
                        f"{target_path}/{upload_filename}", zip_path, token
                    )

                    # 3. Update status to "completed"
                    await update_task_status(
                        task_id,
                        {
                            "status": "completed",
                            "current_step": "completed",
                            "result": result,
                            "step_details": {"message": "Task completed successfully"},
                            "progress": "100",
                        },
                    )

                    logger.info(f"Task {task_id} completed successfully")

                    # 4. Clean up files
                    dzi_path = os.path.splitext(zip_path)[0] + ".dzi"
                    download_dir = os.path.dirname(task_data.get("download_path", ""))

                    await cleanup_files(dzi_path, zip_path, download_dir)

                except Exception as e:
                    logger.error(
                        f"Upload failed for task {task_id}: {str(e)}", exc_info=True
                    )
                    await update_task_status(
                        task_id,
                        {
                            "status": "failed",
                            "current_step": "upload_failed",
                            "error": str(e),
                            "step_details": {"message": f"Upload failed: {str(e)}"},
                        },
                    )

            else:
                await asyncio.sleep(1)  # Brief pause before retrying

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except redis.RedisError as e:
            logger.error(f"Redis error in uploader worker: {e}", exc_info=True)
            await asyncio.sleep(5)  # Wait before retrying connection or operation
        except Exception as e:
            logger.error(f"Unexpected error in uploader worker: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    await get_redis_connection()  # Initialize pool
    logger.info(
        f"Uploader worker application startup complete. Connected to Redis at {REDIS_HOST}:{REDIS_PORT}"
    )
    await worker()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Uploader worker shutting down...")
    finally:
        if redis_pool:
            asyncio.run(redis_pool.disconnect())
        logger.info("Uploader worker shutdown complete.")

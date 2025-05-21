import os
import asyncio
import json
import redis.asyncio as redis
import logging
import sys
import aiohttp
import aiofiles
from datetime import datetime

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DOWNLOAD_QUEUE = "download_queue"
PROCESSING_QUEUE = "processing_queue"

# Constants
DATA_ROOT = "/data"
DOWNLOADS_DIR = os.path.join(DATA_ROOT, "downloads")
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks for streaming download
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(
    total=3600,  # 1 hour total timeout
    connect=60,  # 60 seconds connect timeout
    sock_connect=60,  # 60 seconds to establish connection
    sock_read=300,  # 5 minutes socket read timeout
)

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


async def get_download(path: str, token: str, task_id: str):
    """
    Asynchronous definition for downloading file from EBrains hosted bucket
    Total download time out is 1 hour
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    # Create downloads directory if it doesn't exist
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    download_path = os.path.join(DOWNLOADS_DIR, os.path.basename(path))

    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Getting download URL for {path}")

            # Update task status
            await update_task_status(
                task_id,
                {
                    "status": "processing",
                    "current_step": "downloading",
                    "step_details": {"message": "Getting download URL"},
                    "progress": "5",
                },
            )

            # Get redirect URL
            async with session.get(
                url, headers=headers, timeout=DOWNLOAD_TIMEOUT
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    download_url = data.get("url")
                    if not download_url:
                        raise Exception("Download URL not provided in response")

                    logger.info(
                        f"Starting download from {download_url} to {download_path}"
                    )

                    # Update task status
                    await update_task_status(
                        task_id,
                        {
                            "step_details": {
                                "message": "Starting download",
                                "file": os.path.basename(path),
                            },
                            "progress": "10",
                        },
                    )

                    # Stream download in chunks to handle large files
                    async with session.get(
                        download_url, timeout=DOWNLOAD_TIMEOUT
                    ) as download_response:
                        if download_response.status != 200:
                            raise Exception(
                                f"Failed to download file. Status code: {download_response.status}"
                            )

                        total_size = int(
                            download_response.headers.get("content-length", 0)
                        )
                        downloaded_size = 0
                        last_progress_update = 0

                        async with aiofiles.open(download_path, "wb") as file:
                            async for chunk in download_response.content.iter_chunked(
                                CHUNK_SIZE
                            ):
                                await file.write(chunk)
                                downloaded_size += len(chunk)

                                # Update progress every 10%
                                if total_size > 0:
                                    progress_percent = int(
                                        downloaded_size * 100 / total_size
                                    )
                                    if progress_percent - last_progress_update >= 10:
                                        last_progress_update = progress_percent
                                        download_progress = 10 + int(
                                            progress_percent * 15 / 100
                                        )  # Scale to 10-25% of overall task

                                        # Update task status
                                        await update_task_status(
                                            task_id,
                                            {
                                                "step_details": {
                                                    "message": f"Downloading... {progress_percent}%",
                                                    "file": os.path.basename(path),
                                                    "downloaded": downloaded_size,
                                                    "total": total_size,
                                                },
                                                "progress": str(download_progress),
                                            },
                                        )

                        logger.info(f"Download completed: {download_path}")
                        return download_path
                else:
                    error_msg = (
                        f"Failed to get download URL. Status code: {response.status}"
                    )
                    logger.error(error_msg)
                    raise Exception(error_msg)

        except asyncio.TimeoutError:
            logger.error(f"Timeout during download operation for task {task_id}")
            raise Exception("Download timed out")
        except Exception as e:
            logger.error(f"Download failed for task {task_id}: {str(e)}", exc_info=True)
            raise


async def worker():
    logger.info(f"Downloader worker started. Listening to queue: {DOWNLOAD_QUEUE}")
    r = await get_redis_connection()

    while True:
        try:
            # Blocking pop from the download queue
            message = await r.brpop(DOWNLOAD_QUEUE, timeout=0)
            if message:
                _, task_message_str = message
                task_data = json.loads(task_message_str)
                task_id = task_data.get("task_id")
                path = task_data.get("path")
                target_path = task_data.get("target_path")
                token = task_data.get("token")

                if not all([task_id, path, target_path, token]):
                    logger.error(
                        f"Received task with missing required data: {task_data}"
                    )
                    continue

                logger.info(f"Processing download task {task_id} - path: {path}")

                try:
                    # 1. Download the file
                    download_path = await get_download(path, token, task_id)

                    # 2. Update status for processing
                    await update_task_status(
                        task_id,
                        {
                            "status": "processing",
                            "current_step": "downloaded",
                            "step_details": {
                                "message": "Download completed, queued for processing",
                                "file": os.path.basename(download_path),
                            },
                            "progress": "25",
                        },
                    )

                    # 3. Push to processing queue
                    process_message = {
                        "task_id": task_id,
                        "downloaded_file_path": download_path,
                        "target_path": target_path,
                        "token": token,
                        "download_path": path,  # Keep original path for reference
                    }
                    await r.lpush(PROCESSING_QUEUE, json.dumps(process_message))
                    logger.info(f"Task {task_id} published to {PROCESSING_QUEUE}")

                except Exception as e:
                    logger.error(
                        f"Download failed for task {task_id}: {str(e)}", exc_info=True
                    )
                    await update_task_status(
                        task_id,
                        {
                            "status": "failed",
                            "current_step": "download_failed",
                            "error": str(e),
                            "step_details": {"message": f"Download failed: {str(e)}"},
                        },
                    )
                    # Try to clean up any partial downloads
                    try:
                        if "download_path" in locals() and os.path.exists(
                            download_path
                        ):
                            os.remove(download_path)
                    except Exception as cleanup_error:
                        logger.error(f"Cleanup error: {str(cleanup_error)}")

            else:
                await asyncio.sleep(1)  # Brief pause before retrying

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except redis.RedisError as e:
            logger.error(f"Redis error in downloader worker: {e}", exc_info=True)
            await asyncio.sleep(5)  # Wait before retrying connection or operation
        except Exception as e:
            logger.error(f"Unexpected error in downloader worker: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    await get_redis_connection()  # Initialize pool
    logger.info(
        f"Downloader worker application startup complete. Connected to Redis at {REDIS_HOST}:{REDIS_PORT}"
    )
    await worker()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Downloader worker shutting down...")
    finally:
        if redis_pool:
            asyncio.run(redis_pool.disconnect())
        logger.info("Downloader worker shutdown complete.")

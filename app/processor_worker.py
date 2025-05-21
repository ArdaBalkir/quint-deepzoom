import pyvips
import os
import asyncio
import json
import redis.asyncio as redis
import logging
import sys
from datetime import datetime
import shutil

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PROCESSING_QUEUE = "processing_queue"  # Queue for processing tasks
UPLOAD_QUEUE = "upload_queue"  # Next queue in the pipeline

# Constants
DATA_ROOT = "/data"
DOWNLOADS_DIR = os.path.join(DATA_ROOT, "downloads")
OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")

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


async def deepzoom(path: str):
    """
    Creates a DeepZoom pyramid from the image file specified
    Runs in a thread pool since pyvips operations are CPU-bound
    """

    def process_image():
        logger.info(f"Creating DeepZoom pyramid for {path}")
        image = pyvips.Image.new_from_file(path)
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUTS_DIR, os.path.basename(path))
        image.dzsave(output_path)
        return output_path + ".dzi"

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(process_image)


async def zip_pyramid(path: str):
    """
    Function zips the pyramid files with a .dzip extension
    Runs in a thread pool since compression is CPU-bound
    """
    import zipfile
    from io import BytesIO

    def create_zip():
        dzi_file = path
        dzi_dir = os.path.splitext(dzi_file)[0] + "_files"
        strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])
        zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"

        zip_buffer = BytesIO()
        try:
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zipf:
                zipf.write(dzi_file, os.path.basename(dzi_file))
                for root, _, files in os.walk(dzi_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                        zipf.write(file_path, arcname)

            # Write the buffer to disk only once
            with open(zip_path, "wb") as f:
                f.write(zip_buffer.getvalue())
        finally:
            zip_buffer.close()  # Explicitly close the BytesIO object to free its buffer
        return zip_path

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(create_zip)


async def worker():
    logger.info(f"Processor worker started. Listening to queue: {PROCESSING_QUEUE}")
    r = await get_redis_connection()

    while True:
        try:
            # Blocking pop from the processing queue
            message = await r.brpop(PROCESSING_QUEUE, timeout=0)
            if message:
                _, task_message_str = message
                task_data = json.loads(task_message_str)
                task_id = task_data.get("task_id")
                download_path = task_data.get("downloaded_file_path")
                target_path = task_data.get("target_path")

                if not task_id or not download_path:
                    logger.error(
                        f"Received task without task_id or downloaded_file_path: {task_data}"
                    )
                    continue

                logger.info(f"Processing task {task_id} - file: {download_path}")

                # 1. Update status to "processing DeepZoom"
                await update_task_status(
                    task_id,
                    {
                        "status": "processing",
                        "current_step": "creating_deepzoom",
                        "step_details": {"message": "Creating DeepZoom pyramid"},
                        "progress": "25",
                    },
                )

                # 2. Create DeepZoom pyramid
                try:
                    dzi_path = await deepzoom(download_path)
                    logger.info(f"DeepZoom pyramid created: {dzi_path}")

                    # 3. Update status to "compressing"
                    await update_task_status(
                        task_id,
                        {
                            "current_step": "compressing",
                            "step_details": {"message": "Compressing DeepZoom files"},
                            "progress": "50",
                        },
                    )

                    # 4. Create ZIP file
                    zip_path = await zip_pyramid(dzi_path)
                    logger.info(f"ZIP file created: {zip_path}")

                    # 5. Prepare for upload
                    await update_task_status(
                        task_id,
                        {
                            "current_step": "processed",
                            "step_details": {
                                "message": "Ready for upload",
                                "zip_path": zip_path,
                            },
                            "progress": "75",
                        },
                    )

                    # 6. Push to upload queue
                    upload_message = {
                        "task_id": task_id,
                        "zip_path": zip_path,
                        "target_path": target_path,
                        "token": task_data.get("token"),
                    }
                    await r.lpush(UPLOAD_QUEUE, json.dumps(upload_message))
                    logger.info(
                        f"Task {task_id} published to {UPLOAD_QUEUE}: {upload_message}"
                    )

                except Exception as e:
                    logger.error(
                        f"Processing failed for task {task_id}: {str(e)}", exc_info=True
                    )
                    await update_task_status(
                        task_id,
                        {
                            "status": "failed",
                            "current_step": "processing_failed",
                            "error": str(e),
                            "step_details": {"message": f"Processing failed: {str(e)}"},
                        },
                    )
                    # Optional: Cleanup partial results
                    if "dzi_path" in locals():
                        try:
                            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
                            if os.path.exists(dzi_dir):
                                shutil.rmtree(dzi_dir)
                            if os.path.exists(dzi_path):
                                os.remove(dzi_path)
                        except Exception as cleanup_error:
                            logger.error(f"Cleanup error: {str(cleanup_error)}")

            else:
                await asyncio.sleep(1)  # Brief pause before retrying

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except redis.RedisError as e:
            logger.error(f"Redis error in processor worker: {e}", exc_info=True)
            await asyncio.sleep(5)  # Wait before retrying connection or operation
        except Exception as e:
            logger.error(f"Unexpected error in processor worker: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    await get_redis_connection()  # Initialize pool
    logger.info(
        f"Processor worker application startup complete. Connected to Redis at {REDIS_HOST}:{REDIS_PORT}"
    )
    await worker()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Processor worker shutting down...")
    finally:
        if redis_pool:
            asyncio.run(redis_pool.disconnect())
        logger.info("Processor worker shutdown complete.")

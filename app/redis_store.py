# Redis-based TaskStore implementation for distributed processing
import redis.asyncio as redis
import json
import os
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DOWNLOAD_QUEUE = "download_queue"
PROCESSING_QUEUE = "processing_queue"
UPLOAD_QUEUE = "upload_queue"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class RedisTaskStore:
    """
    Redis-based implementation of task store for distributed processing
    Default TTL is 72 hours
    """

    def __init__(self, ttl_hours=72):
        self.ttl = timedelta(hours=ttl_hours)
        self.redis_pool = None
        logger.info("RedisTaskStore initialized with TTL: %d hours", ttl_hours)

    async def get_redis(self):
        if self.redis_pool is None:
            self.redis_pool = redis.ConnectionPool.from_url(
                f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
            )
        return redis.Redis(connection_pool=self.redis_pool)

    async def add_task(self, task_id: str, task_data: dict):
        r = await self.get_redis()
        now = datetime.now()

        # Format the task data
        task = {
            **task_data,
            "created_at": now.isoformat(),
            "status": "pending",
            "current_step": "initialized",
            "step_details": json.dumps(None),  # Store as JSON string
            "result": json.dumps(None),
            "error": json.dumps(None),
            "progress": "0",  # Store numbers as strings for consistency
        }

        # Convert all values to strings for Redis HSET
        str_task = {k: str(v) for k, v in task.items()}

        # Store task in Redis
        await r.hset(f"task:{task_id}", mapping=str_task)
        # Set expiration
        await r.expire(f"task:{task_id}", int(self.ttl.total_seconds()))

        logger.info(f"Added new task to Redis: {task_id}")

    async def update_task(self, task_id: str, updates: dict):
        r = await self.get_redis()

        # Ensure complex values are JSON strings
        processed_updates = {}
        for k, v in updates.items():
            if isinstance(v, (dict, list)) and k not in ("created_at", "updated_at"):
                processed_updates[k] = json.dumps(v)
            elif isinstance(v, (int, float)) and k != "created_at":
                processed_updates[k] = str(v)
            else:
                processed_updates[k] = v

        # Add updated_at timestamp
        processed_updates["updated_at"] = datetime.now().isoformat()

        if await r.exists(f"task:{task_id}"):
            await r.hset(f"task:{task_id}", mapping=processed_updates)
            # Refresh expiration
            await r.expire(f"task:{task_id}", int(self.ttl.total_seconds()))
            logger.debug(f"Updated task {task_id} in Redis")
        else:
            logger.warning(f"Attempted to update non-existent task {task_id}")

    async def get_task(self, task_id: str) -> Optional[dict]:
        r = await self.get_redis()

        if not await r.exists(f"task:{task_id}"):
            return None

        task_data = await r.hgetall(f"task:{task_id}")

        # Convert JSON strings back to Python objects
        for key in ("step_details", "result", "error"):
            if task_data.get(key):
                try:
                    task_data[key] = json.loads(task_data[key])
                except json.JSONDecodeError:
                    # If not valid JSON, keep as is
                    pass

        # Convert progress to int if possible
        try:
            task_data["progress"] = int(task_data["progress"])
        except (ValueError, KeyError):
            pass

        return task_data

    async def get_all_tasks(self):
        """Get all tasks for admin dashboard"""
        r = await self.get_redis()
        task_keys = await r.keys("task:*")
        tasks = {}

        for key in task_keys:
            task_id = key.split(":", 1)[1]  # Extract task_id from "task:{id}"
            task = await self.get_task(task_id)
            if task:
                tasks[task_id] = task

        return tasks

    async def cleanup_old_tasks(self):
        """
        Redis keys are automatically expired based on TTL,
        but this method can be called to count expired tasks
        """
        all_tasks = await self.get_all_tasks()
        logger.info(f"Current task count: {len(all_tasks)}")
        return len(all_tasks)


# Queue helper functions
async def enqueue_task(queue_name: str, task_data: dict):
    """Add a task to the specified queue"""
    r = await redis.Redis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )
    await r.lpush(queue_name, json.dumps(task_data))
    logger.info(f"Task {task_data.get('task_id')} added to queue {queue_name}")

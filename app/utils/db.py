import sqlite3
from contextlib import contextmanager

DB_PATH = "/data/tasks.db"  # Shared volume path
# The path for the database maybe shifted in the future


@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the SQLite database and create the tasks table if it doesn’t exist."""
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT,
                progress INTEGER,
                result TEXT
            )
        """
        )
        conn.commit()


def update_task_status(task_id, status, progress=None, result=None):
    """Insert or update task status in the database."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO tasks (task_id, status, progress, result)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET status=?, progress=?, result=?
        """,
            (task_id, status, progress, result, status, progress, result),
        )
        conn.commit()


def get_task_status(task_id):
    """Retrieve task status from the database."""
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT status, progress, result FROM tasks WHERE task_id=?", (task_id,)
        )
        row = cursor.fetchone()
        if row:
            return {"status": row[0], "progress": row[1], "result": row[2]}
        else:
            return None

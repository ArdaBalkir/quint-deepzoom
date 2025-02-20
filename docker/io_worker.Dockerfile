FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/celery_app.py app/celery_app.py
COPY app/tasks/io_tasks.py app/tasks/io_tasks.py
COPY app/utils/ app/utils/

CMD ["celery", "-A", "app.celery_app", "worker", "--queues=io_tasks", "--loglevel=info"]
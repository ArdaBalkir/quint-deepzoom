FROM python:3.11-slim

RUN apt-get update && apt-get install -y libvips-dev && apt-get clean
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/celery_app.py app/celery_app.py
COPY app/tasks/processing_tasks.py app/tasks/processing_tasks.py
COPY app/utils/ app/utils/

CMD ["celery", "-A", "app.celery_app", "worker", "--queues=processing_tasks", "--loglevel=info"]
# Builder stage
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only files needed for IO tasks
COPY app/celery_app.py app/celery_app.py
COPY app/tasks/io_tasks.py app/tasks/io_tasks.py
COPY app/utils/ app/utils/

# Final stage
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY --from=builder /app/ .

CMD ["celery", "-A", "app.celery_app", "worker", "--queues=io_tasks", "--loglevel=info"]
from celery import Celery

# Celery instance with RabbitMQ broker and no result backend
app = Celery(
    "deepzoom_tasks",
    broker="amqp://user:password@rabbitmq:5672//",  # Likely to stay 5672 or 73
    backend=None,  # Disable result backend
)

# Celery configuration
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)

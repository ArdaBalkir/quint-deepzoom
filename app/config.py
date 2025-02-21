class Settings:
    RABBITMQ_URL = "amqp://user:password@rabbitmq:5672//"
    REDIS_URL = "redis://redis:6379/0"
    DATA_DIR = "/data"


# The redis is optional, will be further investigated perhaps

settings = Settings()

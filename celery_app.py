from celery import Celery
import os

# ربط سيلري بـ Redis المحلي
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

celery_app = Celery(
    'vidgo_tasks',
    broker=redis_url,
    backend=redis_url,
    imports=['main']  # لتسجيل المهام الموجودة في main.py
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
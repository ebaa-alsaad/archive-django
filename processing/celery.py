import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'archive.settings')

app = Celery('archive')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()

app.conf.beat_schedule = {
    'cleanup-old-files-every-day': {
        'task': 'processing.tasks.cleanup_old_files',
        'schedule': crontab(hour=3, minute=0),  # الساعة 3 صباحاً يومياً
        'args': (7,),  # حذف الملفات الأقدم من 7 أيام
    },
}

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
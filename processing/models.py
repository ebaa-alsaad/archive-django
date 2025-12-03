import os, uuid
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.core.cache import cache
from django.conf import settings

def get_upload_path(instance, filename):
    timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:8]
    return f'uploads/{timestamp}_{unique_id}_{filename}'

class Upload(models.Model):
    STATUS_CHOICES = [
        ('pending','قيد الانتظار'), ('processing','جاري المعالجة'), 
        ('completed','مكتمل'), ('failed','فشل')
    ]
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    original_filename = models.CharField(max_length=255)
    stored_filename = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    progress = models.IntegerField(default=0)
    message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    def update_progress(self, progress, message=""):
        self.progress = progress
        self.message = message
        self.save(update_fields=['progress','message','updated_at'])
        cache.set(f"upload_{self.id}_progress", {
            'progress': progress, 'message': message, 'status': self.status
        }, timeout=3600)

    def set_completed(self):
        self.status = 'completed'
        self.progress = 100
        self.processed_at = timezone.now()
        self.save()


class Group(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE, related_name='groups')
    code = models.CharField(max_length=255)
    pages = models.JSONField(default=list, blank=True)
    pages_count = models.IntegerField(default=0)
    pdf_path = models.CharField(max_length=500, null=True, blank=True)
    filename = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['created_at']

    def __str__(self):
        return f"{self.filename or self.code} ({self.pages_count} صفحات)"

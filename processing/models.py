from django.db import models
from django.contrib.auth.models import User
import os
import uuid
from django.utils import timezone
from django.core.cache import cache

def get_upload_path(instance, filename):
    """تحديد مسار رفع الملف"""
    timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:8]
    return f'uploads/{timestamp}_{unique_id}_{filename}'

class Upload(models.Model):
    STATUS_CHOICES = [
        ('pending', 'قيد الانتظار'),
        ('uploading', 'قيد التحميل'),
        ('processing', 'جاري المعالجة'),
        ('completed', 'مكتمل'),
        ('failed', 'فشل'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    original_filename = models.CharField(max_length=255)
    stored_filename = models.CharField(max_length=255)
    total_pages = models.IntegerField(default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    progress = models.IntegerField(default=0)  # جديد
    message = models.TextField(blank=True)     # جديد
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    processed_at = models.DateTimeField(null=True, blank=True)  # جديد

    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.original_filename} ({self.get_status_display()})"
    
    def get_absolute_path(self):
        from django.conf import settings
        return os.path.join(settings.PRIVATE_MEDIA_ROOT, self.stored_filename)
    
    def update_progress(self, progress, message=""):
        """تحديث التقدم مع caching"""
        self.progress = progress
        self.message = message
        self.save(update_fields=['progress', 'message', 'updated_at'])
        
        # تحديث الـ cache للوصول السريع
        cache_key = f"upload_{self.id}_progress"
        cache.set(cache_key, {
            'progress': progress,
            'message': message,
            'status': self.status
        }, timeout=3600)
    
    def set_completed(self):
        self.status = 'completed'
        self.progress = 100
        self.processed_at = timezone.now()
        self.save()

class Group(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE, related_name='groups')  # تغيير هنا
    code = models.CharField(max_length=255)
    pages = models.JSONField(default=list, blank=True)  # جديد: قائمة بأرقام الصفحات
    pages_count = models.IntegerField(default=0)
    pdf_path = models.CharField(max_length=500, null=True, blank=True)
    filename = models.CharField(max_length=255, blank=True)  # جديد: اسم الملف النهائي
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['created_at']
    
    def __str__(self):
        return f"{self.filename or self.code} ({self.pages_count} صفحات)"

class ProcessingTask(models.Model):
    """تتبع المهام للتعامل مع المعالجة المتوازية"""
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=255, unique=True)
    status = models.CharField(max_length=50, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-created_at']
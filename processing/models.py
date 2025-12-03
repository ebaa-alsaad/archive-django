from django.db import models
from django.contrib.auth.models import User

class Upload(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    original_filename = models.CharField(max_length=255)
    stored_filename = models.CharField(max_length=255)
    total_pages = models.IntegerField(default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class Group(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    upload = models.ForeignKey(Upload, on_delete=models.CASCADE, related_name="groups")
    code = models.CharField(max_length=255)
    pdf_path = models.CharField(max_length=500, null=True, blank=True)
    pages_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

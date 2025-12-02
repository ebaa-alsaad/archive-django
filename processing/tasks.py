import os
from celery import shared_task, current_task
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from .models import Upload, ProcessingTask
from .services import PDFProcessingService
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3, time_limit=1800)
def process_pdf_task(self, upload_id: int):
    """مهمة معالجة PDF خلفية"""
    try:
        logger.info(f"Starting PDF processing task for upload {upload_id}")
        
        # تحديث حالة المهمة
        task_record, created = ProcessingTask.objects.get_or_create(
            task_id=current_task.request.id,
            defaults={
                'upload_id': upload_id,
                'status': 'processing'
            }
        )
        
        # معالجة PDF
        processor = PDFProcessingService(upload_id)
        groups = processor.process()
        
        # تحديث المهمة
        task_record.status = 'completed'
        task_record.completed_at = timezone.now()
        task_record.save()
        
        logger.info(f"PDF processing completed for upload {upload_id}, created {len(groups)} groups")
        
        return {
            'success': True,
            'upload_id': upload_id,
            'groups_count': len(groups),
            'task_id': current_task.request.id
        }
        
    except Exception as e:
        logger.error(f"PDF processing task failed for upload {upload_id}: {e}", exc_info=True)
        
        # تحديث حالة المهمة بالفشل
        try:
            ProcessingTask.objects.filter(task_id=current_task.request.id).update(
                status='failed',
                error_message=str(e)[:500],
                completed_at=timezone.now()
            )
        except:
            pass
        
        # تحديث حالة الرفع
        try:
            upload = Upload.objects.get(id=upload_id)
            upload.status = 'failed'
            upload.message = str(e)[:500]
            upload.save(update_fields=['status', 'message'])
        except:
            pass
        
        raise self.retry(exc=e, countdown=60)

@shared_task
def create_zip_task(upload_id: int):
    """مهمة إنشاء ملف ZIP خلفية"""
    try:
        logger.info(f"Creating ZIP for upload {upload_id}")
        
        upload = Upload.objects.get(id=upload_id)
        groups = upload.groups.all()
        
        if not groups.exists():
            raise ValueError("لا توجد مجموعات متاحة للضغط")
        
        # اسم ملف ZIP
        timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f"archive_{upload.original_filename}_{timestamp}.zip"
        zip_path = Path(settings.PRIVATE_MEDIA_ROOT) / "zips" / zip_filename
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        
        # إنشاء الأرشيف
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for group in groups:
                if group.pdf_path:
                    pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                    if pdf_path.exists():
                        # استخدام اسم المجموعة كاسم الملف داخل الأرشيف
                        arcname = group.filename or f"group_{group.id}.pdf"
                        zipf.write(pdf_path, arcname)
        
        # تخزين مسار ZIP في cache للوصول السريع
        cache_key = f"upload_{upload_id}_zip_path"
        cache.set(cache_key, str(zip_path), timeout=3600)
        
        return {
            'success': True,
            'zip_path': str(zip_path),
            'zip_size': zip_path.stat().st_size
        }
        
    except Exception as e:
        logger.error(f"ZIP creation failed for upload {upload_id}: {e}")
        return {
            'success': False,
            'error': str(e)
        }

@shared_task
def cleanup_old_files(days_old: int = 7):
    """تنظيف الملفات القديمة"""
    try:
        from datetime import timedelta
        
        cutoff_date = timezone.now() - timedelta(days=days_old)
        
        # حذف الرفوعات القديمة
        old_uploads = Upload.objects.filter(
            created_at__lt=cutoff_date,
            status__in=['completed', 'failed']
        )
        
        count = old_uploads.count()
        
        for upload in old_uploads:
            try:
                # حذف الملف الأصلي
                original_path = upload.get_absolute_path()
                if os.path.exists(original_path):
                    os.remove(original_path)
                
                # حذف مجموعات PDF
                groups = upload.groups.all()
                for group in groups:
                    if group.pdf_path:
                        group_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                        if os.path.exists(group_path):
                            os.remove(group_path)
                
                # حذف السجلات
                upload.delete()
                
            except Exception as e:
                logger.warning(f"Failed to delete upload {upload.id}: {e}")
        
        logger.info(f"Cleaned up {count} old uploads")
        return {'deleted_count': count}
        
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        return {'error': str(e)}
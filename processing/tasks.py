# tasks.py
import logging
from .services.barcode_service import BarcodeOCRService
from .models import Upload
from django.utils import timezone

logger = logging.getLogger(__name__)

def process_upload_task(upload_id):
    """
    Standalone task-like function (no celery)
    Runs synchronously inside the request or via thread.
    """

    try:
        upload = Upload.objects.get(id=upload_id)
    except Upload.DoesNotExist:
        logger.error(f"Upload {upload_id} not found")
        return {'success': False, 'reason': 'not_found'}

    logger.info(f"Starting processing for upload {upload_id}")

    # تحديث الحالة
    upload.status = "processing"
    upload.progress = 5
    upload.message = "بدء المعالجة..."
    upload.save(update_fields=['status', 'progress', 'message', 'updated_at'])

    try:
        service = BarcodeOCRService()
        created_groups = service.process_single_pdf(upload)

        upload.status = "completed"
        upload.progress = 100
        upload.message = "تمت المعالجة بنجاح"
        upload.processed_at = timezone.now()
        upload.save()

        logger.info(f"Upload {upload_id} processed successfully")

        return {
            'success': True,
            'groups_created': [g.id for g in created_groups]
        }

    except Exception as e:
        logger.exception(f"Processing failed for upload {upload_id}: {e}")

        upload.status = "failed"
        upload.message = str(e)
        upload.save(update_fields=['status', 'message', 'updated_at'])

        return {
            'success': False,
            'error': str(e)
        }

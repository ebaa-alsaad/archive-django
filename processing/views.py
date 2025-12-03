from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate
from django.utils import timezone
from django.core.cache import cache
from django.conf import settings
from .models import Upload, Group
from .services import BarcodeOCRService
import os
import uuid
from pathlib import Path
import logging
import zipfile

logger = logging.getLogger(__name__)

# ============================
# Upload & Processing Views
# ============================

@login_required
def upload_list(request):
    uploads = Upload.objects.filter(user=request.user).order_by('-created_at')
    return render(request, 'uploads/list.html', {'uploads': uploads})

@login_required
def upload_create(request):
    if request.method == 'POST':
        files = request.FILES.getlist('pdf_file[]')
        uploads = []

        for f in files:
            original_filename = f.name
            unique_id = uuid.uuid4().hex[:8]
            stored_filename = f"{timezone.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}_{original_filename}"

            upload_path = Path(settings.PRIVATE_MEDIA_ROOT) / stored_filename
            upload_path.parent.mkdir(parents=True, exist_ok=True)

            with open(upload_path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)

            upload = Upload.objects.create(
                user=request.user,
                original_filename=original_filename,
                stored_filename=stored_filename,
                status='pending'
            )
            uploads.append(upload)

        return JsonResponse({
            'success': True,
            'uploads': [{'id': u.id, 'name': u.original_filename} for u in uploads]
        })

    return render(request, 'uploads/create.html')

@login_required
def upload_detail(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    groups = upload.groups.all().order_by('created_at')

    cache_key = f"upload_{upload_id}_progress"
    progress_data = cache.get(cache_key) or {
        'progress': upload.progress,
        'message': upload.message or '',
        'status': upload.status
    }

    return render(request, 'uploads/detail.html', {
        'upload': upload,
        'groups': groups,
        'progress': progress_data
    })

@login_required
def download_file(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
    if not path.exists():
        return JsonResponse({'success': False, 'error': 'الملف غير موجود'})
    return FileResponse(open(path, 'rb'), as_attachment=True, filename=upload.original_filename)

@login_required
def process_upload(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)

    if upload.status in ['processing', 'completed']:
        return JsonResponse({'success': False, 'message': 'الملف قيد المعالجة أو مكتمل بالفعل'})

    try:
        upload.status = 'processing'
        upload.save(update_fields=['status'])

        service = BarcodeOCRService()
        service.process_pdf(upload)

        upload.status = 'completed'
        upload.save(update_fields=['status'])
    except Exception as e:
        upload.status = 'failed'
        upload.message = str(e)
        upload.save(update_fields=['status', 'message'])
        return JsonResponse({'success': False, 'message': str(e)})

    return JsonResponse({'success': True, 'message': 'تمت المعالجة بنجاح'})

@login_required
def check_status(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    cache_key = f"upload_{upload_id}_progress"
    progress_data = cache.get(cache_key) or {
        'progress': upload.progress,
        'message': upload.message or '',
        'status': upload.status
    }
    return JsonResponse({
        'success': True,
        'status': upload.status,
        'progress': progress_data['progress'],
        'message': progress_data['message'],
        'groups_count': upload.groups.count()
    })

@login_required
def download_zip(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    if upload.status != 'completed' or upload.groups.count() == 0:
        return JsonResponse({'success': False, 'error': 'لا يمكن تحميل ZIP الآن'})

    zip_path = Path(settings.PRIVATE_MEDIA_ROOT) / f"{upload.id}.zip"

    if not zip_path.exists():
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for group in upload.groups.all():
                group_file = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                if group_file.exists():
                    zipf.write(group_file, arcname=group_file.name)

    zip_filename = f"archive_{upload.original_filename}_{timezone.now().strftime('%Y%m%d')}.zip"
    return FileResponse(open(zip_path, 'rb'), as_attachment=True, filename=zip_filename)

@login_required
def upload_delete(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    try:
        original_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
        if original_path.exists():
            original_path.unlink()
        for group in upload.groups.all():
            if group.pdf_path:
                group_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                if group_path.exists():
                    group_path.unlink()
    except Exception as e:
        logger.warning(f"Failed to delete files for upload {upload_id}: {e}")
    upload.delete()
    return JsonResponse({'success': True, 'message': 'تم الحذف بنجاح'})

@login_required
def download_group_file(request, upload_id, group_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    group = get_object_or_404(Group, id=group_id, upload=upload)

    if not group.pdf_path:
        return JsonResponse({'success': False, 'error': 'الملف غير موجود'})

    pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
    if not pdf_path.exists():
        return JsonResponse({'success': False, 'error': 'الملف غير موجود'})

    filename = group.filename or f"group_{group.id}.pdf"
    return FileResponse(open(pdf_path, 'rb'), as_attachment=True, filename=filename)

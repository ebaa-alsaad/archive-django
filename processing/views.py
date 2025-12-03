import os
import uuid
import logging
import traceback
import zipfile
from pathlib import Path
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate
from django.core.cache import cache
from django.conf import settings
from .models import Upload, Group
from .services import BarcodeOCRService

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

    if request.method == 'GET':
        return render(request, 'uploads/create.html')  

    if request.method != 'POST':
        logger.warning("upload_create: non-POST request", extra={'path': request.path})
        return JsonResponse({'success': False, 'message': 'طريقة الطلب غير صحيحة.'}, status=400)

    logger.debug("upload_create: CONTENT_LENGTH=%s, CONTENT_TYPE=%s, META=%s",
                 request.META.get('CONTENT_LENGTH'), request.META.get('CONTENT_TYPE'),
                 {k: v for k, v in request.META.items() if k.startswith('HTTP_')})

    files = request.FILES.getlist('file')
    if not files:
        logger.warning("upload_create: no files in request.FILES keys=%s", list(request.FILES.keys()))
        return JsonResponse({'success': False, 'message': 'لم يتم إرسال ملفات.'}, status=400)

    uploads = []
    service = BarcodeOCRService()

    for f in files:
        try:
            unique_name = f"{uuid.uuid4().hex}_{f.name}"
            upload_path = Path(settings.PRIVATE_MEDIA_ROOT) / unique_name
            upload_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info("Saving upload file: name=%s size=%s", f.name, getattr(f, 'size', 'unknown'))

            with open(upload_path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)

            upload = Upload.objects.create(
                user=request.user,
                original_filename=f.name,
                stored_filename=unique_name,
                status='pending'
            )
            uploads.append(upload)

            # تعيين الحالة → processing
            upload.status = 'processing'
            upload.save(update_fields=['status'])

            try:
                if hasattr(service, 'process_single_pdf'):
                    service.process_single_pdf(upload)
                else:
                    service.process_pdf(upload)

                upload.set_completed()

            except Exception as exc_proc:
                upload.status = 'failed'
                upload.message = str(exc_proc)
                upload.save(update_fields=['status', 'message'])
                logger.exception("Processing failed for upload %s: %s", upload.id, exc_proc)

        except Exception as e:
            logger.exception("upload_create: failed while handling file %s", getattr(f, 'name', 'unknown'))
            return JsonResponse({
                'success': False,
                'message': 'خطأ أثناء حفظ الملف أو المعالجة.',
                'detail': str(e),
                'trace': traceback.format_exc()
            }, status=500)

    return JsonResponse({
        'success': True,
        'uploads': [
            {'id': u.id, 'name': u.original_filename}
            for u in uploads
        ]
    })


@login_required
def check_status(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    progress_data = cache.get(f"upload_{upload_id}_progress") or {
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

    zip_filename = f"archive_{upload.original_filename}.zip"
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


# ============================
# Dashboard
# ============================

@login_required
def dashboard_view(request):
    uploads_count = Upload.objects.filter(user=request.user).count()
    groups_count = Group.objects.filter(user=request.user).count()
    processing_uploads = Upload.objects.filter(user=request.user, status='processing').count()
    completed_uploads = Upload.objects.filter(user=request.user, status='completed').count()
    latest_uploads = Upload.objects.filter(user=request.user).order_by('-created_at')[:10]

    return render(request, 'dashboard.html', {
        'uploads_count': uploads_count,
        'groups_count': groups_count,
        'processing_uploads': processing_uploads,
        'completed_uploads': completed_uploads,
        'uploads': latest_uploads,
    })


# ============================
# Auth Views
# ============================

def login_view(request):
    if request.method == 'POST':
        username = request.POST.get('username') or request.POST.get('email')
        password = request.POST.get('password')
        user = authenticate(request, username=username, password=password)
        if user:
            login(request, user)
            if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                return JsonResponse({'success': True})
            return redirect('dashboard')
        return render(request, 'auth/login.html', {'error': 'بيانات الدخول غير صحيحة.'})
    return render(request, 'auth/login.html')


def register_view(request):
    if request.method == 'POST':
        username = request.POST.get("username")
        email = request.POST.get("email")
        password = request.POST.get("password")
        password2 = request.POST.get("password2")

        if password != password2:
            return JsonResponse({'success': False, 'message': 'كلمة المرور غير متطابقة.'})
        if User.objects.filter(username=username).exists():
            return JsonResponse({'success': False, 'message': 'اسم المستخدم موجود مسبقاً.'})

        user = User.objects.create_user(username=username, email=email, password=password)
        login(request, user)
        return JsonResponse({'success': True, 'message': 'تم إنشاء الحساب بنجاح!', 'redirect_url': '/dashboard/'})

    return render(request, "auth/register.html")

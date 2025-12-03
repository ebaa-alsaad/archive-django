from django.shortcuts import render, get_object_or_404, redirect 
from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate
from .models import Upload, Group
from .services import BarcodeOCRService
from django.utils import timezone
from django.core.cache import cache
from django.conf import settings
import uuid
import os
from pathlib import Path
import logging

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
            # إنشاء اسم فريد للملف
            original_filename = f.name
            unique_id = uuid.uuid4().hex[:8]
            stored_filename = f"{timezone.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}_{original_filename}"
            
            # مسار الحفظ
            upload_path = Path(settings.PRIVATE_MEDIA_ROOT) / stored_filename
            upload_path.parent.mkdir(parents=True, exist_ok=True)
            
            # حفظ الملف على القرص
            with open(upload_path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)

            # سجل في قاعدة البيانات
            upload = Upload.objects.create(
                user=request.user,
                original_filename=original_filename,
                stored_filename=stored_filename,
                status='pending'
            )
            uploads.append(upload)
            
            # معالجة الملف مباشرة بدون Celery
            try:
                upload.status = 'processing'
                upload.save(update_fields=['status'])
                service = BarcodeOCRService()
                created_groups = service.process_pdf(upload)
                upload.set_completed()
            except Exception as e:
                logger.error(f"Processing failed for upload {upload.id}: {e}")
                upload.status = 'failed'
                upload.message = str(e)
                upload.save(update_fields=['status', 'message'])

        return JsonResponse({
            'success': True,
            'uploads': [{'id': u.id, 'name': u.original_filename} for u in uploads]
        })

    # إذا كان GET، عرض صفحة رفع الملفات
    return render(request, 'uploads/create.html')


@login_required
def upload_detail(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    groups = upload.groups.all().order_by('created_at')
    
    # جلب تقدم المعالجة من cache
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
    path = upload.get_absolute_path()
    if not os.path.exists(path):
        return JsonResponse({'success': False, 'error': 'الملف غير موجود'})
    
    return FileResponse(
        open(path, 'rb'), 
        as_attachment=True, 
        filename=upload.original_filename
    )


@login_required
def check_status(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    
    # جلب التقدم من cache إذا موجود
    cache_key = f"upload_{upload_id}_progress"
    progress_data = cache.get(cache_key)
    
    if not progress_data:
        progress_data = {
            'progress': upload.progress,
            'message': upload.message or '',
            'status': upload.status
        }
    
    response_data = {
        'success': True,
        'status': upload.status,
        'progress': progress_data['progress'],
        'message': progress_data['message'],
        'total_pages': upload.total_pages,
        'groups_count': upload.groups.count()
    }
    
    return JsonResponse(response_data)


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
    
    return FileResponse(
        open(pdf_path, 'rb'),
        as_attachment=True,
        filename=filename
    )


@login_required
def upload_delete(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    
    try:
        original_path = upload.get_absolute_path()
        if os.path.exists(original_path):
            os.remove(original_path)
        
        for group in upload.groups.all():
            if group.pdf_path:
                group_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                if os.path.exists(group_path):
                    os.remove(group_path)
    
    except Exception as e:
        logger.warning(f"Failed to delete files for upload {upload_id}: {e}")
    
    upload.delete()
    
    return JsonResponse({'success': True, 'message': 'تم الحذف بنجاح'})


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

    context = {
        'uploads_count': uploads_count,
        'groups_count': groups_count,
        'processing_uploads': processing_uploads,
        'completed_uploads': completed_uploads,
        'uploads': latest_uploads,
    }
    return render(request, 'dashboard.html', context)


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
        else:
            if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                return JsonResponse({'message': 'بيانات الدخول غير صحيحة.'}, status=400)
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

from django.shortcuts import render, get_object_or_404, redirect 
from django.http import JsonResponse, FileResponse, HttpResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate, logout
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from .models import Upload, Group, ProcessingTask
from .services import PDFProcessingService
from .tasks import process_pdf_task, create_zip_task
from django.conf import settings
from django.contrib import messages
import json
import uuid
import zipfile, os, io, logging 
from pathlib import Path

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
            
            # بدء المعالجة الخلفية
            process_pdf_task.delay(upload.id)

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
def process_upload(request, upload_id):
    """بدء معالجة يدوية للرفع"""
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    
    if upload.status == 'processing':
        return JsonResponse({
            'success': False,
            'error': 'جاري معالجة الملف بالفعل'
        })
    
    if upload.status == 'completed':
        return JsonResponse({
            'success': True,
            'message': 'تم معالجة الملف مسبقاً',
            'groups_count': upload.groups.count()
        })
    
    # تحديث الحالة
    upload.status = 'processing'
    upload.save(update_fields=['status'])
    
    # بدء المهمة الخلفية
    task = process_pdf_task.delay(upload.id)
    
    return JsonResponse({
        'success': True,
        'message': 'بدأت المعالجة',
        'task_id': task.id
    })


@login_required
def check_status(request, upload_id):
    """التحقق من حالة المعالجة"""
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
def download_zip(request, upload_id):
    """تحميل ملف ZIP"""
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    
    if upload.status != 'completed' or upload.groups.count() == 0:
        return JsonResponse({'success': False, 'error': 'لا يمكن تحميل ZIP الآن'})
    
    # التحقق من وجود ملف ZIP أو إنشاء واحد جديد
    cache_key = f"upload_{upload_id}_zip_path"
    zip_path = cache.get(cache_key)
    
    if not zip_path or not os.path.exists(zip_path):
        # إنشاء ZIP جديد
        result = create_zip_task.delay(upload_id)
        # الانتظار حتى يكتمل (في الإنتاج استخدم polling)
        import time
        for _ in range(10):  # 10 محاولات
            if os.path.exists(zip_path):
                break
            time.sleep(1)
    
    zip_path = Path(zip_path) if zip_path else None
    
    if not zip_path or not zip_path.exists():
        # إنشاء ZIP مباشرة
        from .tasks import create_zip_task
        result = create_zip_task(upload_id)
        if not result['success']:
            return JsonResponse({'success': False, 'error': result['error']})
        zip_path = Path(result['zip_path'])
    
    if not zip_path.exists():
        return JsonResponse({'success': False, 'error': 'ملف ZIP غير موجود'})
    
    # اسم ملف ZIP
    zip_filename = f"archive_{upload.original_filename}_{timezone.now().strftime('%Y%m%d')}.zip"
    
    response = FileResponse(open(zip_path, 'rb'))
    response['Content-Type'] = 'application/zip'
    response['Content-Disposition'] = f'attachment; filename="{zip_filename}"'
    
    return response


@login_required
def upload_delete(request, upload_id):
    """حذف رفع"""
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    
    # حذف الملفات
    try:
        original_path = upload.get_absolute_path()
        if os.path.exists(original_path):
            os.remove(original_path)
        
        # حذف مجموعات PDF
        for group in upload.groups.all():
            if group.pdf_path:
                group_path = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                if os.path.exists(group_path):
                    os.remove(group_path)
    
    except Exception as e:
        logger.warning(f"Failed to delete files for upload {upload_id}: {e}")
    
    # حذف السجلات
    upload.delete()
    
    return JsonResponse({
        'success': True,
        'message': 'تم الحذف بنجاح'
    })


@login_required
def download_group_file(request, upload_id, group_id):
    """تحميل ملف مجموعة معينة"""
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


# ============================
# Dashboard
# ============================

@login_required
def dashboard_view(request):
    uploads_count = Upload.objects.filter(user=request.user).count()
    groups_count = Group.objects.filter(user=request.user).count()
    
    # إحصائيات التقدم
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
        if user is not None:
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

        # التحقق من تطابق كلمة المرور
        if password != password2:
            return JsonResponse({'success': False, 'message': 'كلمة المرور غير متطابقة.'})

        # التحقق من اسم المستخدم موجود مسبقاً
        if User.objects.filter(username=username).exists():
            return JsonResponse({'success': False, 'message': 'اسم المستخدم موجود مسبقاً.'})

        # إنشاء المستخدم وتسجيل الدخول
        user = User.objects.create_user(username=username, email=email, password=password)
        login(request, user)
        return JsonResponse({'success': True, 'message': 'تم إنشاء الحساب بنجاح!', 'redirect_url': '/dashboard/'})

    return render(request, "auth/register.html")
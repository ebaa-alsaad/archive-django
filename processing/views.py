import os
import uuid
import logging
import traceback
import zipfile
from pathlib import Path
from threading import Thread
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate
from django.core.cache import cache
from django.conf import settings
from .models import Upload, Group
from .services import BarcodeOCRService
from concurrent.futures import ThreadPoolExecutor
from django.views.decorators.csrf import csrf_exempt
import time

logger = logging.getLogger(__name__)



@login_required
def upload_list(request):
    uploads = Upload.objects.filter(user=request.user).order_by('-created_at')
    return render(request, 'uploads/list.html', {'uploads': uploads})

# ============================
# Upload & Processing Views
# ============================

@login_required
def upload_create(request):
    """رفع الملفات - حفظ الملف بنفس الاسم"""
    if request.method == 'POST':
        try:
            logger.info(f"بدء رفع الملفات - المستخدم: {request.user}")
            
            files = []
            if 'file[]' in request.FILES:
                files = request.FILES.getlist('file[]')
            elif 'file' in request.FILES:
                files = request.FILES.getlist('file')
            
            if not files:
                return JsonResponse({
                    'success': False, 
                    'message': 'لم يتم إرسال ملفات.'
                })

            uploads = []
            for f in files:
                try:
                    logger.info(f"معالجة الملف: {f.name}")
                    
                    # حفظ الملف بنفس الاسم الأصلي
                    original_name = f.name
                    
                    # تأكد من عدم وجود ملف بنفس الاسم
                    upload_dir = Path(settings.PRIVATE_MEDIA_ROOT)
                    upload_dir.mkdir(parents=True, exist_ok=True)
                    
                    # إذا كان الملف موجوداً، أضف timestamp
                    upload_path = upload_dir / original_name
                    counter = 1
                    while upload_path.exists():
                        name_parts = original_name.rsplit('.', 1)
                        new_name = f"{name_parts[0]}_{counter}.{name_parts[1] if len(name_parts) > 1 else 'pdf'}"
                        upload_path = upload_dir / new_name
                        counter += 1
                    
                    # حفظ الملف
                    with open(upload_path, 'wb+') as dest:
                        for chunk in f.chunks():
                            dest.write(chunk)
                    
                    # اسم الملف المخزن (قد يكون مختلفاً إذا كان هناك تكرار)
                    stored_name = upload_path.name
                    
                    # إنشاء سجل في قاعدة البيانات
                    upload = Upload.objects.create(
                        user=request.user,
                        original_filename=original_name,
                        stored_filename=stored_name,  # يحفظ الاسم الفعلي
                        status='pending',
                        progress=0
                    )
                    uploads.append(upload)
                    
                    logger.info(f"تم إنشاء upload: {upload.id} باسم: {stored_name}")
                    
                except Exception as e:
                    logger.error(f"خطأ في معالجة {f.name}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue

            return JsonResponse({
                'success': True, 
                'message': f'تم رفع {len(uploads)} ملف بنجاح',
                'uploads': [{'id': u.id, 'name': u.original_filename} for u in uploads]
            })
            
        except Exception as e:
            logger.error(f"خطأ في upload_create: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': 'حدث خطأ داخلي',
                'error': str(e)
            }, status=500)

    return render(request, 'uploads/create.html')


@login_required
def process_upload(request, upload_id):
    try:
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        if upload.status in ['processing', 'completed']:
            return JsonResponse({
                'success': True, 
                'status': upload.status,
                'message': 'الملف قيد المعالجة أو مكتمل بالفعل'
            })
        
        # تحديث الحالة فوراً
        upload.status = 'processing'
        upload.progress = 5
        upload.save(update_fields=['status', 'progress'])
        
        # استخدام الخدمة الصحيحة
        def background_process():
            try:
                # استيراد الخدمة داخل الدالة لتجنب مشاكل الاستيراد
                from .services import UltraFastBarcodeOCRService
                service = UltraFastBarcodeOCRService()
                service.process_single_pdf(upload)
            except Exception as e:
                logger.error(f"Background processing failed: {e}")
                upload.status = 'failed'
                upload.message = str(e)[:200]
                upload.save(update_fields=['status', 'message'])
        
        # بدء المعالجة في thread منفصل
        thread = threading.Thread(target=background_process, daemon=True)
        thread.start()
        
        return JsonResponse({
            'success': True, 
            'message': 'بدأت المعالجة...',
            'status': 'processing',
            'progress': 5
        })
        
    except Exception as e:
        logger.error(f"Error in process_upload: {e}")
        return JsonResponse({
            'success': False, 
            'message': f'خطأ فوري: {str(e)[:100]}'
        })

@login_required
def check_status(request, upload_id):
    """نسخة مبسطة - لا تسبب ضغط على قاعدة البيانات"""
    try:
        # استخدام cache فقط لتجنب استعلامات قاعدة البيانات المتكررة
        cache_key = f"upload_{upload_id}_status"
        cached_status = cache.get(cache_key)
        
        if cached_status:
            return JsonResponse(cached_status)
        
        # استعلام واحد فقط كل 30 ثانية
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        response_data = {
            'success': True,
            'progress': upload.progress or 0,
            'status': upload.status,
            'message': upload.message or '',
            'groups_count': upload.groups.count()
        }
        
        # تخزين في cache لمدة 30 ثانية
        cache.set(cache_key, response_data, 30)
        
        return JsonResponse(response_data)
    
    except Exception as e:
        return JsonResponse({
            'success': False, 
            'error': str(e),
            'progress': 0,
            'status': 'error'
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

# @login_required
# def process_upload(request, upload_id):
#     upload = get_object_or_404(Upload, id=upload_id, user=request.user)

#     if upload.status in ['processing', 'completed']:
#         return JsonResponse({'success': False, 'message': 'الملف قيد المعالجة أو مكتمل بالفعل'})

#     try:
#         upload.status = 'processing'
#         upload.save(update_fields=['status'])

#         service = BarcodeOCRService()
#         service.process_pdf(upload)

#         upload.status = 'completed'
#         upload.save(update_fields=['status'])
#     except Exception as e:
#         upload.status = 'failed'
#         upload.message = str(e)
#         upload.save(update_fields=['status', 'message'])
#         return JsonResponse({'success': False, 'message': str(e)})

#     return JsonResponse({'success': True, 'message': 'تمت المعالجة بنجاح'})


@login_required
def download_zip(request, upload_id):
    """تحميل ملف ZIP بعد اكتمال المعالجة"""
    try:
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        if upload.status != 'completed':
            return JsonResponse({
                'success': False, 
                'error': 'لم تكتمل المعالجة بعد'
            })
        
        if upload.groups.count() == 0:
            return JsonResponse({
                'success': False, 
                'error': 'لا توجد مجموعات متاحة للتحميل'
            })
        
        # اسم الملف: نفس اسم الملف الأصلي مع .zip
        original_name = upload.original_filename
        if original_name.lower().endswith('.pdf'):
            original_name = original_name[:-4]  # إزالة .pdf
        
        zip_filename = f"{original_name}.zip"
        zip_path = Path(settings.PRIVATE_MEDIA_ROOT) / zip_filename
        
        # حذف ZIP القديم إن وجد
        if zip_path.exists():
            try:
                zip_path.unlink()
            except:
                pass
        
        # إنشاء ملف ZIP
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for group in upload.groups.all():
                if group.pdf_path and group.name:
                    group_file = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                    if group_file.exists():
                        # اسم الملف داخل ZIP
                        arcname = f"{group.name}.pdf"
                        zipf.write(group_file, arcname=arcname)
        
        # إرجاع الملف للتحميل مباشرة
        response = FileResponse(open(zip_path, 'rb'), as_attachment=True, filename=zip_filename)
        response['Content-Type'] = 'application/zip'
        return response
        
    except Exception as e:
        logger.error(f"Error in download_zip: {e}")
        return JsonResponse({
            'success': False, 
            'error': str(e)
        })

@login_required
def auto_download_zip(request, upload_id):
    """فحص إذا كان الملف جاهزاً للتحميل التلقائي"""
    try:
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        if upload.status == 'completed' and upload.groups.count() > 0:
            return JsonResponse({
                'success': True,
                'ready': True,
                'download_url': f'/uploads/{upload.id}/download_zip/',
                'filename': f"{upload.original_filename.replace('.pdf', '')}.zip"
            })
        else:
            return JsonResponse({
                'success': True,
                'ready': False,
                'status': upload.status,
                'groups_count': upload.groups.count()
            })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

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
    context = {'title': 'تسجيل الدخول'} 
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
    context = {'title': 'إنشاء حساب'}
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

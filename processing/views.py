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
    """رفع الملفات - إصلاح خطأ 500"""
    if request.method == 'POST':
        try:
            print(f"DEBUG: بدء رفع الملفات - المستخدم: {request.user}")
            
            # استقبال الملفات
            files = []
            
            # جرب جميع الأسماء الممكنة للملفات
            if 'file[]' in request.FILES:
                files = request.FILES.getlist('file[]')
                print(f"DEBUG: Found files as 'file[]': {len(files)}")
            elif 'file' in request.FILES:
                files = request.FILES.getlist('file')
                print(f"DEBUG: Found files as 'file': {len(files)}")
            else:
                # طباعة جميع المفاتيح المتاحة للتصحيح
                print(f"DEBUG: Available FILES keys: {list(request.FILES.keys())}")
            
            if not files:
                return JsonResponse({
                    'success': False, 
                    'message': 'لم يتم إرسال ملفات.',
                    'debug_info': {
                        'files_keys': list(request.FILES.keys()),
                        'user': str(request.user)
                    }
                })

            uploads = []
            for f in files:
                try:
                    print(f"DEBUG: معالجة الملف: {f.name} - حجم: {f.size} بايت")
                    
                    # إنشاء اسم فريد
                    unique_name = f"{uuid.uuid4().hex}_{f.name}"
                    
                    # تأكد من وجود مجلد الرفع
                    upload_dir = Path(settings.PRIVATE_MEDIA_ROOT)
                    upload_dir.mkdir(parents=True, exist_ok=True)
                    
                    upload_path = upload_dir / unique_name
                    
                    # حفظ الملف
                    with open(upload_path, 'wb+') as dest:
                        for chunk in f.chunks():
                            dest.write(chunk)
                    
                    print(f"DEBUG: تم حفظ الملف في: {upload_path}")
                    
                    # إنشاء سجل في قاعدة البيانات
                    upload = Upload.objects.create(
                        user=request.user,
                        original_filename=f.name,
                        stored_filename=unique_name,
                        status='pending',
                        progress=0
                    )
                    uploads.append(upload)
                    
                    print(f"DEBUG: تم إنشاء سجل upload: {upload.id}")
                    
                except Exception as e:
                    print(f"ERROR: خطأ في معالجة {f.name}: {str(e)}")
                    print(traceback.format_exc())
                    continue

            return JsonResponse({
                'success': True, 
                'message': f'تم رفع {len(uploads)} ملف بنجاح',
                'uploads': [{'id': u.id, 'name': u.original_filename} for u in uploads]
            })
            
        except Exception as e:
            print(f"CRITICAL ERROR في upload_create: {str(e)}")
            print(traceback.format_exc())
            return JsonResponse({
                'success': False,
                'message': 'حدث خطأ داخلي في الخادم',
                'error': str(e)
            }, status=500)

    # GET request - عرض الصفحة فقط
    print(f"DEBUG: GET request لـ upload_create من المستخدم: {request.user}")
    return render(request, 'uploads/create.html')

@login_required
def process_upload(request, upload_id):
    """بدء معالجة الملف """
    
    try:
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        if upload.status in ['processing', 'completed']:
            return JsonResponse({
                'success': True, 
                'message': 'الملف قيد المعالجة أو مكتمل بالفعل'
            })
        
        def quick_process():
            """معالجة سريعة في الخلفية"""
            try:
                # تحديث الحالة بسرعة
                upload.status = 'processing'
                upload.progress = 50
                upload.save(update_fields=['status', 'progress'])
                
                # استدعاء خدمة المعالجة
                service = BarcodeOCRService()
                result = service.process_single_pdf(upload)
                
                # تحديث النتيجة بسرعة
                upload.status = 'completed' if result else 'failed'
                upload.progress = 100 if result else 0
                upload.message = 'تمت المعالجة بنجاح' if result else 'فشلت المعالجة'
                upload.save(update_fields=['status', 'progress', 'message'])
                
                print(f"DEBUG: اكتملت معالجة upload {upload_id}")
                
            except Exception as e:
                upload.status = 'failed'
                upload.message = f'خطأ: {str(e)}'
                upload.save(update_fields=['status', 'message'])
                print(f"ERROR in processing: {e}")
        
        # تشغيل المعالجة في الخلفية بدون انتظار
        thread = Thread(target=quick_process)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            'success': True, 
            'message': 'تم بدء المعالجة'
        })
        
    except Exception as e:
        print(f"ERROR: {e}")
        return JsonResponse({
            'success': False, 
            'message': f'خطأ: {str(e)}'
        })

# إزالة أو تعطيل check_status مؤقتاً للتسريع
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
    """تحميل ملف ZIP بعد اكتمال المعالجة - إصلاح خطأ 'لا يمكن تحميل ZIP الآن'"""
    try:
        upload = get_object_or_404(Upload, id=upload_id, user=request.user)
        
        # تسجيل معلومات التصحيح
        logger.info(f"download_zip called for upload {upload_id}")
        logger.info(f"Upload status: {upload.status}")
        logger.info(f"Groups count: {upload.groups.count()}")
        
        # تحقق مما إذا كان يمكن تحميل ZIP
        if upload.status != 'completed':
            logger.warning(f"Upload {upload_id} status is {upload.status}, not 'completed'")
            return JsonResponse({
                'success': False, 
                'error': f'لم تكتمل المعالجة بعد. الحالة الحالية: {upload.status}'
            })
        
        if upload.groups.count() == 0:
            logger.warning(f"Upload {upload_id} has no groups")
            return JsonResponse({
                'success': False, 
                'error': 'لا توجد مجموعات متاحة للتحميل'
            })
        
        # إنشاء مسار ملف ZIP
        zip_filename = f"{upload.id}_{upload.original_filename}.zip"
        zip_path = Path(settings.PRIVATE_MEDIA_ROOT) / zip_filename
        
        logger.info(f"ZIP path: {zip_path}")
        logger.info(f"ZIP exists before creation: {zip_path.exists()}")
        
        # إذا كان ملف ZIP موجوداً بالفعل، احذفه أولاً
        if zip_path.exists():
            try:
                zip_path.unlink()
                logger.info(f"Deleted existing ZIP file: {zip_path}")
            except Exception as e:
                logger.warning(f"Failed to delete existing ZIP: {e}")
        
        # إنشاء مجلد PRIVATE_MEDIA_ROOT إذا لم يكن موجوداً
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        
        # إنشاء ملف ZIP
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                files_added = 0
                for group in upload.groups.all():
                    if group.pdf_path:
                        group_file = Path(settings.PRIVATE_MEDIA_ROOT) / group.pdf_path
                        if group_file.exists():
                            # استخدام اسم المجموعة أو معرفها كاسم ملف
                            if group.name:
                                arcname = f"{group.name}.pdf"
                            else:
                                arcname = f"group_{group.id}.pdf"
                            
                            zipf.write(group_file, arcname=arcname)
                            files_added += 1
                            logger.info(f"Added to ZIP: {group_file} -> {arcname}")
                        else:
                            logger.warning(f"Group file not found: {group_file}")
                    else:
                        logger.warning(f"Group {group.id} has no pdf_path")
                
                logger.info(f"Total files added to ZIP: {files_added}")
                
                if files_added == 0:
                    return JsonResponse({
                        'success': False, 
                        'error': 'لم يتم العثور على أي ملفات PDF للمجموعات'
                    })
        
        except Exception as e:
            logger.error(f"Error creating ZIP file: {e}")
            return JsonResponse({
                'success': False, 
                'error': f'خطأ في إنشاء ملف ZIP: {str(e)}'
            })
        
        # التحقق من أن ملف ZIP تم إنشاؤه
        if not zip_path.exists():
            logger.error(f"ZIP file was not created: {zip_path}")
            return JsonResponse({
                'success': False, 
                'error': 'فشل إنشاء ملف ZIP'
            })
        
        # الحصول على حجم الملف
        file_size = zip_path.stat().st_size
        logger.info(f"ZIP file created successfully. Size: {file_size} bytes")
        
        # إعداد الاستجابة للتحميل
        response = FileResponse(open(zip_path, 'rb'), as_attachment=True, filename=zip_filename)
        response['Content-Type'] = 'application/zip'
        response['Content-Length'] = file_size
        response['Content-Disposition'] = f'attachment; filename="{zip_filename}"'
        
        return response
        
    except Upload.DoesNotExist:
        logger.error(f"Upload {upload_id} does not exist")
        return JsonResponse({
            'success': False, 
            'error': 'الملف غير موجود'
        })
    except Exception as e:
        logger.error(f"Unexpected error in download_zip: {e}", exc_info=True)
        return JsonResponse({
            'success': False, 
            'error': f'حدث خطأ غير متوقع: {str(e)}'
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

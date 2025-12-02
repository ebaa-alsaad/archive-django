from django.shortcuts import render, get_object_or_404, redirect 
from django.http import JsonResponse, FileResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import login, authenticate, logout
from django.core.files.storage import default_storage
from .models import Upload, Group
from .services import BarcodeOCRService
from django.conf import settings
from django.contrib import messages

import uuid
import zipfile, os, io, logging 

logger = logging.getLogger(__name__)
service = BarcodeOCRService()

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
            # نستخدم اسم الملف كما هو
            stored_name = f.name
            upload_dir = settings.PRIVATE_MEDIA_ROOT
            os.makedirs(upload_dir, exist_ok=True)
            upload_path = os.path.join(upload_dir, stored_name)

            # حفظ الملف على القرص
            with open(upload_path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)

            # سجل في قاعدة البيانات
            upload = Upload.objects.create(
                user=request.user,
                original_filename=f.name,
                stored_filename=stored_name,
                status='pending'
            )
            uploads.append(upload)

        return JsonResponse({
            'success': True,
            'uploads': [{'id': u.id, 'name': u.original_filename} for u in uploads]
        })

    # إذا كان GET، عرض صفحة رفع الملفات
    return render(request, 'uploads/create.html')


@login_required
def upload_detail(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    return render(request, 'uploads/detail.html', {'upload': upload})


@login_required
def download_file(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    path = default_storage.path(upload.stored_filename)
    if not os.path.exists(path):
        return redirect('upload_detail', upload_id=upload.id)
    return FileResponse(open(path, 'rb'), as_attachment=True, filename=upload.original_filename)


@login_required
def process_upload(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    try:
        groups = service.process_pdf(upload)
        upload.status = 'completed'
        upload.total_pages = sum(group.pages_count for group in groups)
        upload.save()
        return JsonResponse({'success': True, 'groups_count': len(groups), 'total_pages': upload.total_pages})
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        upload.status = 'failed'
        upload.save()
        return JsonResponse({'success': False, 'error': str(e)})


@login_required
def check_status(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    return JsonResponse({
        'success': True,
        'status': upload.status,
        'groups_count': upload.groups.count(),
        'total_pages': getattr(upload, 'total_pages', 0)
    })


@login_required
def download_zip(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    if upload.status != 'completed' or upload.groups.count() == 0:
        return JsonResponse({'success': False, 'error': 'لا يمكن تحميل ZIP الآن'})

    zip_io = io.BytesIO()
    with zipfile.ZipFile(zip_io, mode='w') as zipf:
        for group in upload.groups.all():
            pdf_path = default_storage.path(group.pdf_path)
            if os.path.exists(pdf_path):
                zipf.write(pdf_path, os.path.basename(pdf_path))
    zip_io.seek(0)
    return FileResponse(zip_io, as_attachment=True, filename=f"groups_{upload.original_filename}.zip")


@login_required
def upload_delete(request, upload_id):
    upload = get_object_or_404(Upload, id=upload_id, user=request.user)
    if default_storage.exists(upload.stored_filename):
        default_storage.delete(upload.stored_filename)

    for group in upload.groups.all():
        if default_storage.exists(group.pdf_path):
            default_storage.delete(group.pdf_path)
        group.delete()

    upload.delete()
    return JsonResponse({'success': True})


# ============================
# Dashboard
# ============================

@login_required
def dashboard_view(request):
    uploads_count = Upload.objects.count()
    groups_count = Group.objects.count()
    users_count = User.objects.count()
    latest_uploads = Upload.objects.order_by('-created_at')[:6]

    context = {
        'uploads_count': uploads_count,
        'groups_count': groups_count,
        'users_count': users_count,
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
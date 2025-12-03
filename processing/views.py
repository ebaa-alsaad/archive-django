from django.shortcuts import render, redirect
from django.http import JsonResponse
from .models import Upload
from .services import BarcodeOCRService
from django.views.decorators.csrf import csrf_exempt

def upload_page(request):
    uploads = Upload.objects.all().order_by('-created_at')
    return render(request, 'upload_page.html', {'uploads': uploads})

@csrf_exempt
def upload_file(request):
    if request.method == 'POST' and request.FILES.get('file'):
        f = request.FILES['file']
        upload = Upload.objects.create(
            user=request.user,
            stored_filename=f.name,
            original_filename=f.name,
            status='processing'
        )
        # حفظ الملف فعليًا في MEDIA_ROOT
        with open(f'{upload.get_file_path()}', 'wb+') as destination:
            for chunk in f.chunks():
                destination.write(chunk)
        # استدعاء المعالجة بشكل غير متزامن (يمكن استخدام Celery أو threading)
        from threading import Thread
        Thread(target=BarcodeOCRService().process_pdf, args=(upload,)).start()
        return JsonResponse({'upload_id': upload.id})
    return JsonResponse({'error': 'No file uploaded'}, status=400)

def get_progress(request, upload_id):
    from django.core.cache import cache
    progress = cache.get(f"upload_progress:{upload_id}", 0)
    message = cache.get(f"upload_message:{upload_id}", "")
    return JsonResponse({'progress': progress, 'message': message})

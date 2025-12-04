import os
import re
import hashlib
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from pdf2image import convert_from_path
import pytesseract
from pyzbar.pyzbar import decode as decode_barcode
from django.core.cache import cache
from .models import Upload, Group
from django.conf import settings

logger = logging.getLogger(__name__)

class BarcodeOCRService:
    """
    خدمة معالجة PDF محسّنة:
    - تقسيم صفحات حسب الباركود
    - استخراج النص باستخدام OCR
    - إنشاء PDF لكل مجموعة
    - تحسين الأداء عبر pdf2image وThreadPoolExecutor
    """

    def __init__(self):
        self.text_cache = {}
        self.ocr_cache = {}
        self.image_cache = {}
        self.barcode_cache = {}

    def process_single_pdf(self, upload):
        """معالجة ملف PDF واحد وتحديث حالة الـ Upload"""
        upload_id = upload.id
        logger.info(f"Start processing upload {upload_id}")
        
        try:
            pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")

            # تحديث الحالة
            upload.status = 'processing'
            upload.progress = 30
            upload.save(update_fields=['status', 'progress'])
            
            # حذف أي مجموعات سابقة
            Group.objects.filter(upload=upload).delete()

            # تحويل الصفحات إلى صور
            images = convert_from_path(str(pdf_path), dpi=150, thread_count=4)

            # قراءة الباركود للفصل
            separator_barcode = self.read_barcode_from_image(images[0]) or "default_barcode"

            # تقسيم الصفحات حسب الباركود
            sections, current_section = [], []
            for i, img in enumerate(images):
                barcode = self.read_barcode_from_image(img)
                if barcode == separator_barcode:
                    if current_section:
                        sections.append(current_section)
                    current_section = []
                else:
                    current_section.append(i)
            if current_section:
                sections.append(current_section)

            upload.progress = 60
            upload.save(update_fields=['progress'])

            # إنشاء ملفات PDF لكل مجموعة
            created_groups = []
            for idx, pages in enumerate(sections):
                if not pages:
                    continue
                    
                # استخراج النص لإنشاء اسم المجموعة
                text = self.extract_text_from_image(images[pages[0]])
                
                # البحث عن: رقم قيد، رقم فاتورة، رقم سند، تاريخ
                filename = None
                patterns = [
                    (r'رقم\s*القيد\s*[:]?\s*(\d+)', 'قيد'),
                    (r'رقم\s*الفاتورة\s*[:]?\s*(\d+)', 'فاتورة'),
                    (r'رقم\s*السند\s*[:]?\s*(\d+)', 'سند'),
                    (r'تاريخ\s*[:]?\s*(\d{4}[-/]\d{2}[-/]\d{2})', 'تاريخ'),
                    (r'(\d{4}[-/]\d{2}[-/]\d{2})', 'تاريخ'),  # أي تاريخ
                ]
                
                for pattern, prefix in patterns:
                    m = re.search(pattern, text)
                    if m:
                        filename = f"{prefix}_{m.group(1)}"
                        break
                
                # إذا لم نجد شيئاً، استخدم الباركود ورقم المجموعة
                if not filename:
                    filename = f"{separator_barcode}_{idx+1}"
                
                # تنظيف اسم الملف
                filename = self.sanitize_filename(filename)
                filename_safe = f"{filename}.pdf"
                
                # مجلد المجموعات
                output_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / filename_safe

                # إنشاء PDF للمجموعة
                import fitz
                new_doc = fitz.open()
                orig_doc = fitz.open(pdf_path)
                for p in pages:
                    new_doc.insert_pdf(orig_doc, from_page=p, to_page=p)
                new_doc.save(output_path)
                new_doc.close()
                orig_doc.close()

                # حفظ المجموعة في قاعدة البيانات
                group = Group.objects.create(
                    code=separator_barcode,
                    pdf_path=f"groups/{filename_safe}",
                    pages_count=len(pages),
                    user=upload.user,
                    upload=upload,
                    filename=filename_safe,
                    name=filename  # حفظ الاسم المستخرج
                )
                created_groups.append(group)

                # تحديث التقدم
                progress = 70 + int((idx + 1) / len(sections) * 30)
                upload.progress = progress
                upload.save(update_fields=['progress'])

            # تحديث الحالة النهائية
            upload.status = 'completed'
            upload.progress = 100
            upload.message = f'تمت المعالجة بنجاح. عدد المجموعات: {len(created_groups)}'
            upload.save(update_fields=['status', 'progress', 'message'])
            
            # حذف الملف الأصلي بعد المعالجة
            try:
                if pdf_path.exists():
                    pdf_path.unlink()
                    logger.info(f"تم حذف الملف الأصلي: {pdf_path}")
            except Exception as e:
                logger.warning(f"فشل حذف الملف الأصلي: {e}")
            
            logger.info(f"Processing complete for upload {upload_id}. Groups created: {len(created_groups)}")
            return created_groups
            
        except Exception as e:
            # تحديث الحالة إلى failed
            upload.status = 'failed'
            upload.message = f'خطأ في المعالجة: {str(e)}'
            upload.save(update_fields=['status', 'message'])
            logger.error(f"Error processing upload {upload_id}: {e}")
            raise
    
    # -----------------------------
    # دوال مساعدة
    # -----------------------------
    
    def read_barcode_from_image(self, img):
        """قراءة الباركود من الصورة"""
        if img in self.barcode_cache:
            return self.barcode_cache[img]
        barcode = None
        for obj in decode_barcode(img):
            barcode = obj.data.decode("utf-8")
            break
        self.barcode_cache[img] = barcode
        return barcode

    def extract_text_from_image(self, img):
        """استخراج النص من الصورة باستخدام OCR"""
        if img in self.ocr_cache:
            return self.ocr_cache[img]
        text = pytesseract.image_to_string(img, lang="ara").strip()
        self.ocr_cache[img] = text
        return text

    def generate_filename(self, images, page_number, barcode, idx):
        """إنشاء اسم ملف للمجموعة"""
        text = self.extract_text_from_image(images[page_number])
        for pattern in [r'سند\s*[:\-]?\s*(\d+)', r'قيد\s*[:\-]?\s*(\d+)', r'(\d{4}-\d{2}-\d{2})']:
            m = re.search(pattern, text)
            if m:
                return self.sanitize_filename(m.group(1))
        return self.sanitize_filename(f"{barcode}_{idx+1}")

    def sanitize_filename(self, filename):
        """تنظيف اسم الملف من الأحرف غير المسموحة"""
        filename = re.sub(r'[^\w\-_\.]', '_', filename)
        return filename or f"file_{os.getpid()}"

    def process_multiple_pdfs(self, uploads, max_workers=4):
        """معالجة عدة ملفات PDF بالتزامن"""
        results = {}
        
        def process_upload(upload):
            try:
                groups = self.process_single_pdf(upload)
                return upload.id, {"success": True, "groups": groups}
            except Exception as e:
                return upload.id, {"success": False, "error": str(e)}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_upload, upload) for upload in uploads]
            
            for future in futures:
                upload_id, result = future.result()
                results[upload_id] = result
        
        return results
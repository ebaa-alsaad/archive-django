import os
import re
import hashlib
import logging
import subprocess
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
        
        # التحقق من poppler
        self._check_poppler_installed()
        
    def _check_poppler_installed(self):
        """التحقق من تثبيت poppler في النظام"""
        try:
            # التحقق من وجود pdftoppm
            result = subprocess.run(['which', 'pdftoppm'], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError("poppler-utils not installed. Run: sudo apt install poppler-utils")
            
            # التحقق من وجود pdfinfo
            result = subprocess.run(['pdfinfo', '--version'], 
                                  capture_output=True, text=True)
            logger.info(f"Poppler version found: {result.stdout.strip()}")
            
            # تعيين مسار poppler لـ pdf2image
            poppler_path = result.stdout.split()[1] if 'Poppler' in result.stdout else None
            if poppler_path:
                os.environ['POPPLER_PATH'] = poppler_path
                
        except Exception as e:
            logger.error(f"Poppler check failed: {e}")
            raise RuntimeError(f"Poppler check failed: {e}")

    def process_single_pdf(self, upload):
        """معالجة ملف PDF واحد وتحديث حالة الـ Upload"""
        upload_id = upload.id
        logger.info(f"Start processing upload {upload_id}")
        
        try:
            pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")

            # التحقق من أن الملف PDF صالح
            self._validate_pdf_file(pdf_path)

            # تحديث الحالة
            upload.status = 'processing'
            upload.progress = 30
            upload.save(update_fields=['status', 'progress'])
            
            # حذف أي مجموعات سابقة
            Group.objects.filter(upload=upload).delete()

            # تحويل الصفحات إلى صور مع تعيين مسار poppler صراحةً
            images = convert_from_path(
                str(pdf_path), 
                dpi=150, 
                thread_count=2,  # قلل عدد الثريدات لتجنب مشاكل الذاكرة
                poppler_path=self._get_poppler_path()
            )
            
            logger.info(f"Converted PDF to {len(images)} images")

            if not images:
                raise RuntimeError("Failed to convert PDF to images")

            # قراءة الباركود للفصل
            separator_barcode = self.read_barcode_from_image(images[0]) or "default_barcode"
            logger.info(f"Separator barcode: {separator_barcode}")

            # تقسيم الصفحات حسب الباركود
            sections, current_section = [], []
            for i, img in enumerate(images):
                barcode = self.read_barcode_from_image(img)
                if barcode == separator_barcode:
                    if current_section:
                        sections.append(current_section)
                        logger.info(f"Section {len(sections)}: {len(current_section)} pages")
                    current_section = []
                else:
                    current_section.append(i)
            if current_section:
                sections.append(current_section)
                logger.info(f"Last section {len(sections)}: {len(current_section)} pages")

            upload.progress = 60
            upload.save(update_fields=['progress'])
            logger.info(f"Found {len(sections)} sections in PDF")

            # إنشاء ملفات PDF لكل مجموعة
            created_groups = []
            for idx, pages in enumerate(sections):
                if not pages:
                    continue
                    
                # استخراج النص لإنشاء اسم المجموعة
                text = self.extract_text_from_image(images[pages[0]])
                logger.info(f"Section {idx+1} OCR text preview: {text[:100]}...")
                
                # البحث عن: رقم قيد، رقم فاتورة، رقم سند، تاريخ
                filename = None
                patterns = [
                    (r'رقم\s*القيد\s*[:]?\s*(\d+)', 'قيد'),
                    (r'رقم\s*الفاتورة\s*[:]?\s*(\d+)', 'فاتورة'),
                    (r'رقم\s*السند\s*[:]?\s*(\d+)', 'سند'),
                    (r'تاريخ\s*[:]?\s*(\d{4}[-/]\d{2}[-/]\d{2})', 'تاريخ'),
                    (r'(\d{4}[-/]\d{2}[-/]\d{2})', 'تاريخ'),
                ]
                
                for pattern, prefix in patterns:
                    m = re.search(pattern, text)
                    if m:
                        filename = f"{prefix}_{m.group(1)}"
                        logger.info(f"Found pattern '{prefix}' with value: {m.group(1)}")
                        break
                
                # إذا لم نجد شيئاً، استخدم الباركود ورقم المجموعة
                if not filename:
                    filename = f"{separator_barcode}_{idx+1}"
                    logger.info(f"Using default filename: {filename}")
                
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
                
                logger.info(f"Created group PDF: {output_path} with {len(pages)} pages")

                # حفظ المجموعة في قاعدة البيانات
                group = Group.objects.create(
                    code=separator_barcode,
                    pdf_path=f"groups/{filename_safe}",
                    pages_count=len(pages),
                    user=upload.user,
                    upload=upload,
                    filename=filename_safe,
                    name=filename
                )
                created_groups.append(group)
                logger.info(f"Created group in database: {group.id}")

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
            logger.error(f"Error processing upload {upload_id}: {e}", exc_info=True)
            raise
    
    # -----------------------------
    # دوال مساعدة محسنة
    # -----------------------------
    
    def _validate_pdf_file(self, pdf_path):
        """التحقق من أن الملف PDF صالح"""
        try:
            import fitz
            doc = fitz.open(pdf_path)
            page_count = doc.page_count
            doc.close()
            logger.info(f"PDF validation: {pdf_path} has {page_count} pages")
            return True
        except Exception as e:
            logger.error(f"Invalid PDF file {pdf_path}: {e}")
            raise RuntimeError(f"Invalid PDF file: {e}")
    
    def _get_poppler_path(self):
        """الحصول على مسار poppler"""
        # جرب المسارات الشائعة
        possible_paths = [
            '/usr/bin',
            '/usr/local/bin',
            '/opt/homebrew/bin',  # macOS
            '/usr/lib/x86_64-linux-gnu',  # Ubuntu
        ]
        
        for path in possible_paths:
            pdftoppm_path = os.path.join(path, 'pdftoppm')
            if os.path.exists(pdftoppm_path):
                logger.info(f"Found poppler at: {path}")
                return path
        
        # إذا لم نجد، استخدم الافتراضي
        logger.warning("Using default poppler path (system PATH)")
        return None
    
    def read_barcode_from_image(self, img):
        """قراءة الباركود من الصورة"""
        try:
            if img in self.barcode_cache:
                return self.barcode_cache[img]
            
            # تحويل الصورة إلى تنسيق مناسب للباركود
            import cv2
            import numpy as np
            
            if hasattr(img, 'mode') and img.mode == 'RGBA':
                img = img.convert('RGB')
            
            img_array = np.array(img)
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            
            barcode = None
            for obj in decode_barcode(gray):
                barcode = obj.data.decode("utf-8", errors='ignore')
                logger.info(f"Found barcode: {barcode}")
                break
                
            self.barcode_cache[img] = barcode
            return barcode
            
        except Exception as e:
            logger.warning(f"Barcode reading failed: {e}")
            return None

    def extract_text_from_image(self, img):
        """استخراج النص من الصورة باستخدام OCR"""
        try:
            if img in self.ocr_cache:
                return self.ocr_cache[img]
            
            # تحسين الصورة للـ OCR
            import cv2
            import numpy as np
            
            img_array = np.array(img)
            
            # تحويل إلى رمادي
            if len(img_array.shape) == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array
            
            # تحسين التباين
            gray = cv2.equalizeHist(gray)
            
            # التحويل مرة أخرى إلى صورة PIL
            from PIL import Image
            enhanced_img = Image.fromarray(gray)
            
            text = pytesseract.image_to_string(enhanced_img, lang="ara+eng").strip()
            self.ocr_cache[img] = text
            return text
            
        except Exception as e:
            logger.warning(f"OCR failed: {e}")
            return ""

    def sanitize_filename(self, filename):
        """تنظيف اسم الملف من الأحرف غير المسموحة"""
        filename = re.sub(r'[^\w\-_\.]', '_', filename)
        filename = filename.strip('_.')
        return filename or f"file_{os.getpid()}_{hash(filename) % 10000}"

    def process_multiple_pdfs(self, uploads, max_workers=2):
        """معالجة عدة ملفات PDF بالتزامن"""
        results = {}
        
        def process_upload(upload):
            try:
                groups = self.process_single_pdf(upload)
                return upload.id, {"success": True, "groups": groups}
            except Exception as e:
                logger.error(f"Failed to process upload {upload.id}: {e}")
                return upload.id, {"success": False, "error": str(e)}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_upload, upload) for upload in uploads]
            
            for future in futures:
                upload_id, result = future.result()
                results[upload_id] = result
        
        return results
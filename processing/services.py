import os
import re
import hashlib
import logging
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from pdf2image import convert_from_path
import pytesseract
from pyzbar.pyzbar import decode as decode_barcode
import cv2
import numpy as np
from PIL import Image
import fitz  # PyMuPDF - الإصدار الجديد
from django.core.cache import cache
from .models import Upload, Group
from django.conf import settings
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class UltraFastBarcodeOCRService:
    """
    خدمة معالجة PDF فائقة السرعة باستخدام PyMuPDF:
    - اكتشاف باركود مباشر من PDF بدون تحويل للصور
    - تقسيم ذكي للصفحات
    - معالجة متوازية متقدمة
    - تحسين الذاكرة والأداء
    """

    def __init__(self):
        self._poppler_path = self._find_poppler_path()
        self._lock = threading.Lock()
        self._barcode_cache = {}
        
        # إعدادات الأداء
        self.OCR_ENABLED = False  # تعطيل OCR للسرعة إلا إذا احتجنا إليه
        self.MIN_PAGES_FOR_SAMPLING = 50
        self.MAX_WORKERS = min(4, os.cpu_count() or 2)
        
    def _find_poppler_path(self):
        """العثور على مسار poppler"""
        for path in ['/usr/bin', '/usr/local/bin', '/usr/lib/x86_64-linux-gnu']:
            if os.path.exists(os.path.join(path, 'pdftoppm')):
                return path
        return None

    def process_single_pdf(self, upload):
        """معالجة PDF واحدة - فائقة السرعة"""
        upload_id = upload.id
        start_time = time.time()
        logger.info(f"🚀 بدء معالجة فائقة السرعة لـ upload {upload_id}")
        
        try:
            pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")

            # الحالة: بدء المعالجة
            with self._lock:
                upload.status = 'processing'
                upload.progress = 10
                upload.save(update_fields=['status', 'progress'])
            
            # ===== الخطوة 1: فتح PDF وتحليل سريع =====
            logger.info(f"📖 فتح الملف: {pdf_path}")
            doc = fitz.open(pdf_path)
            total_pages = doc.page_count
            
            logger.info(f"📄 عدد الصفحات: {total_pages}")
            
            upload.progress = 20
            upload.save(update_fields=['progress'])
            
            # ===== الخطوة 2: اكتشاف الباركودات الذكي =====
            separator_barcode = self._find_separator_barcode_fast(doc, total_pages)
            logger.info(f"🔍 باركود الفصل: {separator_barcode}")
            
            upload.progress = 40
            upload.save(update_fields=['progress'])
            
            # ===== الخطوة 3: تقسيم الصفحات السريع =====
            sections = self._split_pages_fast(doc, separator_barcode, total_pages)
            logger.info(f"📊 عدد الأقسام: {len(sections)}")
            
            upload.progress = 60
            upload.save(update_fields=['progress'])
            
            # ===== الخطوة 4: إنشاء المجموعات بالتوازي =====
            Group.objects.filter(upload=upload).delete()
            
            created_groups = self._create_groups_ultra_fast(doc, sections, separator_barcode, upload)
            
            # إغلاق الوثيقة
            doc.close()
            
            # ===== الخطوة 5: تحديث الحالة النهائية =====
            processing_time = time.time() - start_time
            logger.info(f"⏱️ وقت المعالجة: {processing_time:.2f} ثانية")
            
            with self._lock:
                upload.status = 'completed'
                upload.progress = 100
                upload.message = f'تمت المعالجة في {processing_time:.1f} ثانية. المجموعات: {len(created_groups)}'
                upload.save(update_fields=['status', 'progress', 'message'])
            
            # حذف الملف الأصلي
            self._delete_original_if_needed(pdf_path)
            
            logger.info(f"✅ اكتملت المعالجة لـ upload {upload_id}. المجموعات: {len(created_groups)}")
            return created_groups
            
        except Exception as e:
            logger.error(f"❌ خطأ في معالجة upload {upload_id}: {e}", exc_info=True)
            with self._lock:
                upload.status = 'failed'
                upload.message = f'خطأ في المعالجة: {str(e)[:100]}'
                upload.save(update_fields=['status', 'message'])
            raise
    
    def _find_separator_barcode_fast(self, doc, total_pages):
        """اكتشاف سريع للباركود الفاصل"""
        # استراتيجية ذكية للعثور على الباركود
        check_pages = []
        
        # الصفحة الأولى هي الأهم
        check_pages.append(0)
        
        # بعض الصفحات الوسطى
        if total_pages > 10:
            check_pages.append(total_pages // 2)
        
        # الصفحة الأخيرة
        if total_pages > 1:
            check_pages.append(total_pages - 1)
        
        # الصفحات 2-6 (غالباً بها باركود فاصل)
        for i in range(1, min(6, total_pages)):
            check_pages.append(i)
        
        # فحص الصفحات المختارة
        for page_num in check_pages:
            try:
                barcode = self._extract_barcode_from_pdf_page(doc, page_num)
                if barcode and barcode.strip():
                    logger.info(f"✅ وجد باركود في الصفحة {page_num}: {barcode}")
                    return barcode
            except Exception as e:
                logger.debug(f"لا يوجد باركود في الصفحة {page_num}: {e}")
                continue
        
        # إذا لم نجد باركوداً، نستخدم اسم الملف
        default_code = doc.name.split('/')[-1].split('.')[0][:20] or "document"
        logger.info(f"⚠️ استخدام باركود افتراضي: {default_code}")
        return default_code
    
    def _extract_barcode_from_pdf_page(self, doc, page_num, dpi=72):
        """استخراج باركود من صفحة PDF مباشرة"""
        try:
            page = doc[page_num]
            
            # استخراج الصورة من PDF بدقة منخفضة للسرعة
            pix = page.get_pixmap(dpi=dpi)
            
            # تحويل إلى مصفوفة numpy مباشرة (بدون PIL)
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            
            # إذا كانت الصورة ملونة (RGB)
            if pix.n == 4:  # RGBA
                # تجاهل قناة ألفا
                img_array = img_array[:, :, :3]
            
            # تحويل إلى رمادي
            if len(img_array.shape) == 3 and img_array.shape[2] == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array
            
            # اكتشاف الباركود
            barcodes = decode_barcode(gray)
            for barcode in barcodes:
                return barcode.data.decode("utf-8", errors='ignore')
            
            # إذا لم نجد باركوداً، جرب استخراج النص
            try:
                text = page.get_text("text")
                if text:
                    # بحث عن أنماط باركود في النص
                    patterns = [
                        r'\b\d{8,15}\b',  # أرقام طويلة (مثل الباركود)
                        r'CODE[\s:]*(\d+)',
                        r'باركود[\s:]*(\d+)',
                        r'Barcode[\s:]*(\d+)',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, text, re.IGNORECASE)
                        if matches:
                            return matches[0]
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.debug(f"فشل استخراج الباركود من الصفحة {page_num}: {e}")
            return None
    
    def _split_pages_fast(self, doc, separator_barcode, total_pages):
        """تقسيم سريع للصفحات باستخدام استراتيجية ذكية"""
        sections = []
        current_section = []
        
        # تقليل عدد الصفحات التي نفحصها
        if total_pages > 100:
            # للملفات الكبيرة: فحص عينة فقط
            step = max(1, total_pages // 50)  # فحص 2% من الصفحات
            pages_to_check = list(range(0, total_pages, step))
            logger.info(f"🔍 فحص عينة من {len(pages_to_check)} صفحة من أصل {total_pages}")
        else:
            # للملفات الصغيرة: فحص كل الصفحات
            pages_to_check = range(total_pages)
        
        for i, page_num in enumerate(pages_to_check):
            try:
                # التحقق من الباركود
                barcode = self._extract_barcode_from_pdf_page(doc, page_num, dpi=50)  # دقة منخفضة للسرعة
                
                if barcode == separator_barcode:
                    if current_section:
                        # إضافة القسم مع توسيعه ليشمل الصفحات المفقودة
                        full_section = self._expand_section(current_section, pages_to_check, page_num, total_pages)
                        sections.append(full_section)
                        logger.debug(f"➕ قسم جديد: {len(full_section)} صفحة")
                    current_section = []
                else:
                    current_section.append(page_num)
                    
            except Exception as e:
                logger.debug(f"خطأ في فحص الصفحة {page_num}: {e}")
                current_section.append(page_num)  # أضفها رغم الخطأ
        
        # إضافة القسم الأخير
        if current_section:
            full_section = self._expand_section(current_section, pages_to_check, total_pages, total_pages)
            sections.append(full_section)
        
        # ترتيب وتنظيف الأقسام
        cleaned_sections = []
        for section in sections:
            if section:  # تجاهل الأقسام الفارغة
                # إزالة التكرارات وترتيب الصفحات
                unique_pages = sorted(list(set(section)))
                if unique_pages:  # تأكد من أن القسم غير فارغ
                    cleaned_sections.append(unique_pages)
        
        return cleaned_sections
    
    def _expand_section(self, section_indices, checked_indices, break_point, total_pages):
        """توسيع قسم ليشمل جميع الصفحات"""
        if not section_indices:
            return []
        
        # العثور على نطاق الصفحات
        start_page = min(section_indices)
        end_page = max(section_indices)
        
        # إذا كان break_point قبل end_page، استخدمه
        if break_point < end_page and break_point > start_page:
            end_page = break_point - 1
        
        # التوسيع
        expanded = []
        for page_num in range(start_page, min(end_page + 1, total_pages)):
            expanded.append(page_num)
        
        return expanded
    
    def _create_groups_ultra_fast(self, doc, sections, separator_barcode, upload):
        """إنشاء المجموعات بأقصى سرعة"""
        created_groups = []
        output_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        def create_single_group(idx, pages):
            """إنشاء مجموعة واحدة"""
            try:
                if not pages:
                    return None
                
                # اسم الملف
                timestamp = datetime.now().strftime("%H%M%S")
                filename = f"{separator_barcode}_{idx+1}_{timestamp}"
                filename = self._sanitize_filename(filename)
                filename_safe = f"{filename}.pdf"
                output_path = output_dir / filename_safe
                
                # إنشاء PDF جديد
                new_doc = fitz.open()
                for page_num in pages:
                    if page_num < doc.page_count:
                        new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
                
                # حفظ مع ضغط متقدم
                new_doc.save(
                    output_path,
                    deflate=True,        # ضغط
                    garbage=4,          # تنظيف
                    clean=True,         # تنظيف الهيكل
                    deflate_images=True, # ضغط الصور
                    deflate_fonts=True  # ضغط الخطوط
                )
                new_doc.close()
                
                # إضافة إلى قاعدة البيانات
                group = Group.objects.create(
                    code=separator_barcode,
                    pdf_path=f"groups/{filename_safe}",
                    pages_count=len(pages),
                    user=upload.user,
                    upload=upload,
                    filename=filename_safe,
                    name=filename
                )
                
                return group
                
            except Exception as e:
                logger.error(f"❌ فشل إنشاء المجموعة {idx+1}: {e}")
                return None
        
        # معالجة بالتوازي مع تحديث التقدم
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = []
            for idx, pages in enumerate(sections):
                future = executor.submit(create_single_group, idx, pages)
                futures.append(future)
            
            # جمع النتائج مع تحديث التقدم
            completed = 0
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        created_groups.append(result)
                        completed += 1
                        
                        # تحديث التقدم
                        if len(sections) > 0:
                            progress = 60 + int((completed / len(sections)) * 40)
                            with self._lock:
                                upload.progress = min(progress, 99)
                                upload.save(update_fields=['progress'])
                                
                except Exception as e:
                    logger.error(f"❌ خطأ في معالجة قسم: {e}")
        
        return created_groups
    
    def _sanitize_filename(self, filename):
        """تنظيف اسم الملف"""
        # إزالة الأحرف غير الآمنة
        filename = re.sub(r'[^\w\-_\.]', '_', filename)
        # تقصير إذا طال
        if len(filename) > 80:
            name, ext = os.path.splitext(filename)
            filename = name[:75] + ext
        return filename or "document"
    
    def _delete_original_if_needed(self, pdf_path):
        """حذف الملف الأصلي إذا كانت المعالجة ناجحة"""
        try:
            if pdf_path.exists():
                # تحقق من حجم الملف أولاً
                file_size = pdf_path.stat().st_size
                if file_size > 50 * 1024 * 1024:  # أكبر من 50MB
                    logger.info(f"⚠️ الاحتفاظ بالملف الأصلي الكبير: {file_size / (1024*1024):.1f}MB")
                    return
                
                pdf_path.unlink()
                logger.info(f"🗑️ تم حذف الملف الأصلي: {pdf_path}")
        except Exception as e:
            logger.warning(f"⚠️ فشل حذف الملف الأصلي: {e}")

    def process_multiple_pdfs_async(self, uploads):
        """معالجة عدة ملفات بشكل غير متزامن"""
        import asyncio
        import aiohttp
        
        async def process_upload_async(upload):
            """معالجة upload واحدة بشكل غير متزامن"""
            try:
                groups = await asyncio.to_thread(self.process_single_pdf, upload)
                return upload.id, {"success": True, "groups": groups}
            except Exception as e:
                return upload.id, {"success": False, "error": str(e)}
        
        async def main():
            """الدالة الرئيسية للمعالجة المتوازية"""
            tasks = [process_upload_async(upload) for upload in uploads]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            processed_results = {}
            for result in results:
                if isinstance(result, tuple) and len(result) == 2:
                    upload_id, data = result
                    processed_results[upload_id] = data
            
            return processed_results
        
        # تشغيل في loop جديد
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(main())
        finally:
            loop.close()
        
        return results
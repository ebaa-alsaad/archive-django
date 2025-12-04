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

class BarcodeOCRService:
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
        self.current_upload = upload
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
                upload.progress = 5
                upload.message = 'جاري تهيئة الملف...'
                upload.save(update_fields=['status', 'progress', 'message'])
            
            # ===== الخطوة 1: فتح PDF وتحليل =====
            logger.info(f"📖 فتح الملف: {pdf_path}")
            doc = fitz.open(pdf_path)
            total_pages = doc.page_count
            
            logger.info(f"📄 عدد الصفحات: {total_pages}")
            
            self._update_upload_progress(upload, 25, f'تم تحميل {total_pages} صفحة')
            
            # ===== الخطوة 2: اكتشاف الباركود الفاصل =====
            separator_barcode = self._find_separator_barcode_fast(doc, total_pages)
            logger.info(f"🔍 باركود الفصل: {separator_barcode}")
            
            self._update_upload_progress(upload, 30, f'تم تحديد الباركود الفاصل')
            
            # ===== الخطوة 3: تقسيم الصفحات =====
            self._update_upload_progress(upload, 35, 'جاري تقسيم الصفحات إلى أقسام...')
            sections = self._split_pages_fast(doc, separator_barcode, total_pages)
            
            if not sections:
                raise Exception("لم يتم العثور على أقسام - ربما الباركود الفاصل غير صحيح")
            
            logger.info(f"📊 عدد الأقسام: {len(sections)}")
            self._update_upload_progress(upload, 50, f'تم تقسيم الملف إلى {len(sections)} قسم')
            
            # ===== الخطوة 4: إنشاء المجموعات =====
            Group.objects.filter(upload=upload).delete()
            
            self._update_upload_progress(upload, 60, 'جاري إنشاء ملفات PDF للمجموعات...')
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
            
            # حذف الملف الأصلي (اختياري)
            # self._delete_original_if_needed(pdf_path)
            
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
        
        # محاولة استخراج النص أولاً (أسرع)
        text = page.get_text("text")
        if text:
            # البحث عن أنماط الباركود في النص
            patterns = [
                r'\b\d{8,20}\b',  # أرقام من 8 إلى 20 رقم
                r'باركود[\s:]*(\d+)',
                r'Barcode[\s:]*(\d+)',
                r'Code[\s:]*(\d+)',
                r'رقم[\s:]*(\d+)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE | re.ARABIC)
                if matches:
                    barcode = str(matches[0]).strip()
                    if len(barcode) >= 8:  # تأكد أنه باركود حقيقي
                        logger.debug(f"📄 وجد باركود في النص (صفحة {page_num}): {barcode}")
                        return barcode
        
        # إذا لم نجد في النص، نبحث في الصورة
        pix = page.get_pixmap(dpi=dpi)
        
        # تحويل إلى مصفوفة numpy
        img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
        
        # تحويل إلى رمادي
        if len(img_array.shape) == 3 and img_array.shape[2] == 3:
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
        else:
            gray = img_array
        
        # اكتشاف الباركود
        barcodes = decode_barcode(gray)
        for barcode in barcodes:
            barcode_text = barcode.data.decode("utf-8", errors='ignore').strip()
            if barcode_text:
                logger.debug(f"📷 وجد باركود في الصورة (صفحة {page_num}): {barcode_text}")
                return barcode_text
        
        return None
        
    except Exception as e:
        logger.debug(f"❌ فشل استخراج الباركود من الصفحة {page_num}: {e}")
        return None


    def _split_pages_fast(self, doc, separator_barcode, total_pages):
    """تقسيم الصفحات - إصلاح بناءً على كود Laravel"""
    sections = []
    current_section = []
    
    logger.info(f"🔍 بدء تقسيم {total_pages} صفحة باستخدام باركود: {separator_barcode}")
    
    for page_num in range(total_pages):
        try:
            # استخراج الباركود من الصفحة
            barcode = self._extract_barcode_from_pdf_page(doc, page_num)
            
            # تحديث التقدم
            progress = 40 + ((page_num + 1) / total_pages * 20)
            if page_num % 10 == 0:  # تحديث كل 10 صفحات
                with self._lock:
                    self._update_upload_progress(self.current_upload, progress, f"جاري معالجة الصفحة {page_num + 1} من {total_pages}...")
            
            # المقارنة الدقيقة للباركود (مثل كود Laravel)
            if barcode and str(barcode).strip() == str(separator_barcode).strip():
                # ⭐ المفتاح: إذا وجدنا باركود فاصل، ننهي القسم الحالي إذا لم يكن فارغاً
                if current_section:
                    sections.append(current_section.copy())
                    logger.debug(f"➕ قسم جديد {len(sections)}: الصفحات {current_section}")
                    
                    # تحديث الحالة
                    with self._lock:
                        self._update_upload_progress(self.current_upload, progress, 
                            f"تم إنشاء {len(sections)} قسم حتى الآن...")
                
                current_section = []  # ابدأ قسم جديد فارغ ⭐ لا تضيف صفحة الباركود
                logger.debug(f"🔗 صفحة باركود فاصل: {page_num} - بدء قسم جديد")
            else:
                # صفحة عادية - أضفها للقسم الحالي
                current_section.append(page_num)
                
        except Exception as e:
            logger.debug(f"خطأ في فحص الصفحة {page_num}: {e}")
            current_section.append(page_num)  # أضفها رغم الخطأ
    
    # ⭐ إضافة آخر قسم إذا لم يكن فارغاً (مثل كود Laravel)
    if current_section:
        sections.append(current_section)
        logger.debug(f"➕ قسم نهائي {len(sections)}: الصفحات {current_section}")
    
    # تصفية الأقسام الفارغة
    cleaned_sections = [section for section in sections if section]
    
    logger.info(f"✅ تم تقسيم الصفحات إلى {len(cleaned_sections)} قسم")
    for i, section in enumerate(cleaned_sections):
        logger.info(f"   القسم {i+1}: الصفحات {section}")
    
    return cleaned_sections

    def _update_upload_progress(self, upload, progress, message=''):
    """تحديث حالة التقدم - مشابه لـ Laravel"""
    if upload:
        try:
            upload.progress = int(progress)
            if hasattr(upload, 'message'):
                upload.message = message
            upload.save(update_fields=['progress', 'message'])
            
            # تخزين في cache للوصول السريع
            from django.core.cache import cache
            cache_key = f"upload_progress_{upload.id}"
            cache.set(cache_key, {
                'progress': progress,
                'message': message,
                'timestamp': time.time()
            }, 300)  # 5 دقائق
            
            logger.debug(f"📊 تحديث التقدم: {progress}% - {message}")
        except Exception as e:
            logger.warning(f"❌ فشل تحديث التقدم: {e}")



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
    """إنشاء المجموعات مع استخراج الأسماء من النص"""
    created_groups = []
    output_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_group_name_from_page(page_num):
        """استخراج اسم المجموعة من أول صفحة - مشابه لـ Laravel"""
        try:
            page = doc[page_num]
            text = page.get_text("text")
            
            if not text or len(text.strip()) < 10:
                return None
            
            # البحث عن رقم السند (مثل Laravel)
            patterns = [
                r'رقم\s*السند\s*[:\-]?\s*(\d{2,})',
                r'السند\s*[:\-]?\s*(\d{2,})',
                r'سند\s*[:\-]?\s*(\d{2,})',
                r'سند\s*رقم\s*[:\-]?\s*(\d{2,})',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE | re.ARABIC)
                if matches:
                    return f"سند_{matches[0]}"
            
            # البحث عن رقم القيد
            qeed_patterns = [
                r'رقم\s*القيد\s*[:\-]?\s*(\d+)',
                r'القيد\s*[:\-]?\s*(\d+)',
                r'قيد\s*[:\-]?\s*(\d+)',
            ]
            
            for pattern in qeed_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE | re.ARABIC)
                if matches:
                    return f"قيد_{matches[0]}"
            
            # البحث عن تاريخ
            date_patterns = [
                r'(\d{2}/\d{2}/\d{4})',
                r'(\d{2}-\d{2}-\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
            ]
            
            for pattern in date_patterns:
                matches = re.findall(pattern, text)
                if matches:
                    return f"تاريخ_{matches[0].replace('/', '-')}"
            
            return None
            
        except Exception as e:
            logger.debug(f"فشل استخراج الاسم من الصفحة {page_num}: {e}")
            return None
    
    def create_single_group(idx, pages):
        """إنشاء مجموعة واحدة"""
        try:
            if not pages:
                return None
            
            # استخراج اسم المجموعة من أول صفحة
            group_name = extract_group_name_from_page(pages[0])
            
            # إذا لم نجد اسماً، نستخدم اسم افتراضي
            if not group_name:
                group_name = f"{separator_barcode}_{idx+1}"
            
            # تنظيف الاسم
            group_name = self._sanitize_filename(group_name)
            filename_safe = f"{group_name}.pdf"
            output_path = output_dir / filename_safe
            
            # إنشاء PDF جديد
            new_doc = fitz.open()
            for page_num in pages:
                if page_num < doc.page_count:
                    new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            
            # حفظ مع ضغط
            new_doc.save(output_path, deflate=True, garbage=4, clean=True)
            new_doc.close()
            
            # التحقق من حجم الملف
            if not output_path.exists() or output_path.stat().st_size < 10000:  # أقل من 10KB
                logger.warning(f"📄 ملف صغير جداً: {output_path} ({output_path.stat().st_size} bytes)")
                return None
            
            # إضافة إلى قاعدة البيانات
            group = Group.objects.create(
                code=separator_barcode,
                pdf_path=f"groups/{filename_safe}",
                pages_count=len(pages),
                user=upload.user,
                upload=upload,
                filename=filename_safe,
                name=group_name
            )
            
            logger.info(f"✅ تم إنشاء المجموعة {idx+1}: {group_name} ({len(pages)} صفحة)")
            return group
            
        except Exception as e:
            logger.error(f"❌ فشل إنشاء المجموعة {idx+1}: {e}")
            return None
    
    # معالجة بالتوازي مع تحديث التقدم
    total_sections = len(sections)
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
                    progress = 60 + int((completed / total_sections) * 40)
                    self._update_upload_progress(upload, progress, 
                        f"تم إنشاء {completed} من {total_sections} مجموعة...")
                        
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
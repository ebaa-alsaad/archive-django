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
    """تقسيم سريع للصفحات باستخدام استراتيجية ذكية"""
        sections = []
        current_section = []
        
        # فحص كل الصفحات (مهم لفصل المجموعات بدقة)
        for page_num in range(total_pages):
            try:
                # استخراج باركود من الصفحة
                barcode = self._extract_barcode_from_pdf_page(doc, page_num, dpi=72)
                
                # إذا كانت هذه الصفحة تحتوي على باركود الفاصل
                if barcode == separator_barcode:
                    # هذه صفحة فاصل - نبدأ مجموعة جديدة
                    if current_section:
                        # نحفظ المجموعة الحالية
                        sections.append(current_section.copy())
                        current_section = []
                    # لا نضيف صفحة الباركود الفاصل للمجموعة
                    continue
                else:
                    # صفحة عادية - نضيفها للمجموعة الحالية
                    current_section.append(page_num)
                    
            except Exception as e:
                logger.debug(f"خطأ في فحص الصفحة {page_num}: {e}")
                # في حالة خطأ، نضيف الصفحة للمجموعة الحالية
                current_section.append(page_num)
        
        # إضافة آخر مجموعة إذا كانت موجودة
        if current_section:
            sections.append(current_section)
        
        # تصفية المجموعات الفارغة
        cleaned_sections = [section for section in sections if section]
        
        logger.info(f"🔢 تم تقسيم الصفحات إلى {len(cleaned_sections)} مجموعة")
        for i, section in enumerate(cleaned_sections):
            logger.info(f"   المجموعة {i+1}: الصفحات {section}")
        
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
    
    def extract_group_name(page_num):
        """استخراج اسم المجموعة من أول صفحة"""
        try:
            page = doc[page_num]
            text = page.get_text("text")
            if text:
                # البحث عن أسماء محتملة
                patterns = [
                    r'رقم[:\s]*(\d+)',  # رقم القيد
                    r'رقم السند[:\s]*(\d+)',
                    r'الفاتورة رقم[:\s]*(\d+)',
                    r'Invoice[:\s]*(\d+)',
                    r'(\d{2}/\d{2}/\d{4})',  # تاريخ
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE | re.ARABIC)
                    if matches:
                        return matches[0]
                
                # إذا لم نجد، نستخدم أول سطر من النص
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and len(line) > 3 and not line.isnumeric():
                        return line[:50]  # تقصير إذا كان طويلاً
        except:
            pass
        
        # اسم افتراضي
        return f"مجموعة_{page_num+1}"
    
    def create_single_group(idx, pages):
        """إنشاء مجموعة واحدة"""
        try:
            if not pages:
                return None
            
            # استخراج اسم المجموعة من أول صفحة
            group_name = extract_group_name(pages[0])
            
            # اسم الملف
            timestamp = datetime.now().strftime("%H%M%S")
            filename = f"{group_name}_{idx+1}_{timestamp}"
            filename = self._sanitize_filename(filename)
            filename_safe = f"{filename}.pdf"
            output_path = output_dir / filename_safe
            
            # إنشاء PDF جديد
            new_doc = fitz.open()
            for page_num in pages:
                if page_num < doc.page_count:
                    new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            
            # حفظ مع ضغط
            new_doc.save(output_path, deflate=True, garbage=4, clean=True)
            new_doc.close()
            
            # إضافة إلى قاعدة البيانات
            group = Group.objects.create(
                code=separator_barcode,
                pdf_path=f"groups/{filename_safe}",
                pages_count=len(pages),
                user=upload.user,
                upload=upload,
                filename=filename_safe,
                name=group_name  # حفظ الاسم المستخرج
            )
            
            logger.info(f"✅ تم إنشاء المجموعة {idx+1}: {group_name} ({len(pages)} صفحة)")
            return group
            
        except Exception as e:
            logger.error(f"❌ فشل إنشاء المجموعة {idx+1}: {e}")
            return None
    
    # معالجة بالتوازي
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
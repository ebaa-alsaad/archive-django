import os
import re
import hashlib
import subprocess
import logging
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from .models import Upload, Group
import shutil
from typing import List, Tuple, Optional, Dict

logger = logging.getLogger(__name__)

class PDFProcessingService:
    """خدمة معالجة PDF متقدمة مع تحسين الأداء"""
    
    def __init__(self, upload_id: int, max_workers: int = None):
        self.upload_id = upload_id
        self.upload = Upload.objects.get(id=upload_id)
        self.pdf_path = Path(self.upload.get_absolute_path())
        self.pdf_hash = hashlib.md5(str(self.pdf_path).encode()).hexdigest()
        
        # إعداد العمال
        self.max_workers = max_workers or max(2, os.cpu_count() - 1)
        
        # مجلدات التخزين المؤقت
        self.temp_dir = Path(tempfile.mkdtemp(dir=settings.TEMP_ROOT))
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # ذاكرة التخزين المؤقت في الذاكرة
        self.memory_cache = {}
        
        logger.info(f"Initialized PDFProcessingService for upload {upload_id}")
    
    def __del__(self):
        """تنظيف الملفات المؤقتة"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.warning(f"Failed to clean temp directory: {e}")
    
    def process(self) -> List[Group]:
        """المعالجة الرئيسية"""
        try:
            # تحديث الحالة
            self.upload.status = 'processing'
            self.upload.save(update_fields=['status'])
            self._update_progress(5, "جاري تحميل الملف...")
            
            # التحقق من وجود الملف
            if not self.pdf_path.exists():
                raise FileNotFoundError(f"PDF file not found: {self.pdf_path}")
            
            # الحصول على عدد الصفحات
            total_pages = self._get_page_count()
            self.upload.total_pages = total_pages
            self.upload.save(update_fields=['total_pages'])
            
            self._update_progress(10, f"تم تحميل {total_pages} صفحة")
            
            # تحديد الباركود الفاصل
            separator_barcode = self._read_barcode(1)
            if not separator_barcode:
                separator_barcode = "SEPARATOR"
                logger.warning("No barcode found on first page, using default separator")
            
            self._update_progress(15, "جاري تحديد المجموعات...")
            
            # تحديد المجموعات (sections)
            sections = self._identify_sections(separator_barcode, total_pages)
            
            if not sections:
                raise ValueError("لم يتم العثور على أي مجموعات في الملف")
            
            self._update_progress(20, f"تم تحديد {len(sections)} مجموعة")
            
            # حذف المجموعات القديمة
            Group.objects.filter(upload=self.upload).delete()
            
            # معالجة المجموعات بالتوازي
            created_groups = self._process_sections_parallel(sections, separator_barcode)
            
            # تحديث حالة الرفع
            self.upload.set_completed()
            self._update_progress(100, f"تم إنشاء {len(created_groups)} ملف بنجاح")
            
            return created_groups
            
        except Exception as e:
            logger.error(f"Processing failed for upload {self.upload_id}: {e}", exc_info=True)
            self.upload.status = 'failed'
            self.upload.message = str(e)
            self.upload.save(update_fields=['status', 'message'])
            raise
    
    def _identify_sections(self, separator_barcode: str, total_pages: int) -> List[List[int]]:
        """تحديد المجموعات بناءً على الباركود الفاصل"""
        sections = []
        current_section = []
        
        # قراءة الباركود لكل صفحة بالتوازي
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # إنشاء مهام لقراءة الباركود
            futures = {}
            for page_num in range(1, total_pages + 1):
                future = executor.submit(self._read_barcode, page_num)
                futures[future] = page_num
            
            # معالجة النتائج بالترتيب
            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    barcode = future.result()
                    
                    # إذا كان الباركود هو الفاصل، نبدأ مجموعة جديدة
                    if barcode == separator_barcode:
                        if current_section:
                            sections.append(current_section)
                            current_section = []
                    else:
                        current_section.append(page_num)
                        
                except Exception as e:
                    logger.error(f"Error reading barcode for page {page_num}: {e}")
                    # نعتبر الصفحة بدون باركود جزءًا من المجموعة الحالية
                    if not separator_barcode:
                        current_section.append(page_num)
        
        # إضافة المجموعة الأخيرة
        if current_section:
            sections.append(current_section)
        
        return sections
    
    def _process_sections_parallel(self, sections: List[List[int]], separator_barcode: str) -> List[Group]:
        """معالجة المجموعات بالتوازي"""
        created_groups = []
        total_sections = len(sections)
        
        with ProcessPoolExecutor(max_workers=min(self.max_workers, total_sections)) as executor:
            futures = {}
            
            for idx, section_pages in enumerate(sections, 1):
                # حساب التقدم
                progress = 20 + (70 * idx // total_sections)
                self._update_progress(progress, f"جاري معالجة المجموعة {idx} من {total_sections}")
                
                # إرسال المهمة للمعالجة
                future = executor.submit(
                    self._process_single_section,
                    section_pages,
                    idx,
                    separator_barcode
                )
                futures[future] = idx
            
            # جمع النتائج
            for future in as_completed(futures):
                try:
                    group = future.result()
                    if group:
                        created_groups.append(group)
                except Exception as e:
                    logger.error(f"Error processing section: {e}")
        
        return created_groups
    
    def _process_single_section(self, pages: List[int], section_idx: int, separator_barcode: str) -> Optional[Group]:
        """معالجة مجموعة واحدة"""
        try:
            if not pages:
                return None
            
            # إنشاء اسم الملف من المحتوى
            filename = self._generate_filename(pages[0])
            
            # إذا لم يتم استخراج اسم مناسب، استخدام ترقيم
            if not filename or filename == "unknown":
                filename = f"{separator_barcode}_{section_idx:03d}"
            
            # تنظيف اسم الملف
            safe_filename = self._sanitize_filename(filename)
            output_filename = f"{safe_filename}.pdf"
            
            # مسار حفظ الملف
            output_path = Path(settings.PRIVATE_MEDIA_ROOT) / "groups" / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # إنشاء PDF للمجموعة
            if self._create_pdf_from_pages(pages, output_path):
                # إنشاء كائن Group في قاعدة البيانات
                group = Group.objects.create(
                    user=self.upload.user,
                    upload=self.upload,
                    code=separator_barcode,
                    filename=output_filename,
                    pages=pages,
                    pages_count=len(pages),
                    pdf_path=f"groups/{output_filename}"
                )
                return group
            
            return None
            
        except Exception as e:
            logger.error(f"Error in _process_single_section: {e}")
            return None
    
    def _generate_filename(self, page_num: int) -> str:
        """توليد اسم الملف من محتوى الصفحة"""
        # محاولة استخراج النص باستخدام pdftotext أولاً (أسرع)
        text = self._extract_text_with_pdftotext(page_num)
        
        if text:
            # البحث عن رقم السند
            saned_match = re.search(r'سند\s*[:\-]?\s*(\d{2,})', text)
            if saned_match:
                return f"سند_{saned_match.group(1)}"
            
            # البحث عن رقم الفاتورة
            invoice_match = re.search(r'فاتورة\s*[:\-]?\s*(\d{2,})', text, re.IGNORECASE)
            if invoice_match:
                return f"فاتورة_{invoice_match.group(1)}"
            
            # البحث عن رقم القيد
            qeed_match = re.search(r'قيد\s*[:\-]?\s*(\d+)', text)
            if qeed_match:
                return f"قيد_{qeed_match.group(1)}"
            
            # البحث عن تاريخ
            date = self._extract_date_from_text(text)
            if date:
                return f"وثيقة_{date}"
        
        # إذا فشل pdftotext، استخدم OCR
        ocr_text = self._extract_text_with_ocr(page_num)
        if ocr_text and len(ocr_text) > 20:
            # تكرار نفس البحث على نص OCR
            for pattern, prefix in [
                (r'سند\s*[:\-]?\s*(\d{2,})', 'سند'),
                (r'فاتورة\s*[:\-]?\s*(\d{2,})', 'فاتورة'),
                (r'قيد\s*[:\-]?\s*(\d+)', 'قيد')
            ]:
                match = re.search(pattern, ocr_text, re.IGNORECASE)
                if match:
                    return f"{prefix}_{match.group(1)}"
            
            date = self._extract_date_from_text(ocr_text)
            if date:
                return f"وثيقة_{date}"
        
        return "unknown"
    
    def _extract_text_with_pdftotext(self, page_num: int) -> Optional[str]:
        """استخراج النص باستخدام pdftotext (سريع)"""
        cache_key = f"{self.pdf_hash}_pdftotext_{page_num}"
        
        if cache_key in self.memory_cache:
            return self.memory_cache[cache_key]
        
        # التحقق من cache الملف
        cache_file = self.cache_dir / f"{cache_key}.txt"
        if cache_file.exists():
            text = cache_file.read_text(encoding='utf-8', errors='ignore')
            self.memory_cache[cache_key] = text
            return text
        
        try:
            # استخدام pdftotext
            temp_output = self.temp_dir / f"text_{page_num}.txt"
            cmd = [
                'pdftotext',
                '-f', str(page_num),
                '-l', str(page_num),
                '-layout',
                str(self.pdf_path),
                str(temp_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if temp_output.exists():
                text = temp_output.read_text(encoding='utf-8', errors='ignore')
                text = re.sub(r'\s+', ' ', text).strip()
                
                # حفظ في cache
                cache_file.write_text(text, encoding='utf-8')
                self.memory_cache[cache_key] = text
                
                # تنظيف الملف المؤقت
                temp_output.unlink(missing_ok=True)
                
                return text
        
        except Exception as e:
            logger.warning(f"pdftotext failed for page {page_num}: {e}")
        
        return None
    
    def _extract_text_with_ocr(self, page_num: int) -> Optional[str]:
        """استخراج النص باستخدام OCR (أبطأ ولكن أكثر دقة)"""
        cache_key = f"{self.pdf_hash}_ocr_{page_num}"
        
        if cache_key in self.memory_cache:
            return self.memory_cache[cache_key]
        
        cache_file = self.cache_dir / f"{cache_key}.txt"
        if cache_file.exists():
            text = cache_file.read_text(encoding='utf-8', errors='ignore')
            self.memory_cache[cache_key] = text
            return text
        
        try:
            # تحويل الصفحة إلى صورة
            image_path = self._convert_page_to_image(page_num)
            if not image_path or not image_path.exists():
                return None
            
            # استخدام Tesseract للـ OCR
            output_base = self.temp_dir / f"ocr_{page_num}"
            cmd = [
                'tesseract',
                str(image_path),
                str(output_base),
                '-l', 'ara+eng',
                '--psm', '6',
                '--oem', '3'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            text_file = output_base.with_suffix('.txt')
            if text_file.exists():
                text = text_file.read_text(encoding='utf-8', errors='ignore')
                text = re.sub(r'\s+', ' ', text).strip()
                
                # حفظ في cache
                cache_file.write_text(text, encoding='utf-8')
                self.memory_cache[cache_key] = text
                
                # تنظيف الملفات المؤقتة
                text_file.unlink(missing_ok=True)
                image_path.unlink(missing_ok=True)
                
                return text
        
        except Exception as e:
            logger.warning(f"OCR failed for page {page_num}: {e}")
        
        return None
    
    def _convert_page_to_image(self, page_num: int) -> Optional[Path]:
        """تحويل صفحة PDF إلى صورة"""
        cache_key = f"{self.pdf_hash}_image_{page_num}"
        cache_file = self.cache_dir / f"{cache_key}.png"
        
        if cache_file.exists():
            return cache_file
        
        try:
            output_base = self.temp_dir / f"page_{page_num}"
            cmd = [
                'pdftoppm',
                '-f', str(page_num),
                '-l', str(page_num),
                '-png',
                '-singlefile',
                str(self.pdf_path),
                str(output_base)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            image_path = output_base.with_suffix('.png')
            if image_path.exists():
                # نسخ إلى cache
                shutil.copy2(image_path, cache_file)
                return cache_file
        
        except Exception as e:
            logger.warning(f"Failed to convert page {page_num} to image: {e}")
        
        return None
    
    def _read_barcode(self, page_num: int) -> Optional[str]:
        """قراءة الباركود من صفحة"""
        cache_key = f"{self.pdf_hash}_barcode_{page_num}"
        
        if cache_key in self.memory_cache:
            return self.memory_cache[cache_key]
        
        cache_file = self.cache_dir / f"{cache_key}.txt"
        if cache_file.exists():
            barcode = cache_file.read_text(encoding='utf-8').strip()
            self.memory_cache[cache_key] = barcode
            return barcode
        
        try:
            # تحويل الصفحة إلى صورة أولاً
            image_path = self._convert_page_to_image(page_num)
            if not image_path:
                return None
            
            # استخدام zbarimg لقراءة الباركود
            cmd = ['zbarimg', '--quiet', '--raw', str(image_path)]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                barcode = result.stdout.strip()
                
                # حفظ في cache
                cache_file.write_text(barcode, encoding='utf-8')
                self.memory_cache[cache_key] = barcode
                
                return barcode
        
        except Exception as e:
            logger.warning(f"Failed to read barcode from page {page_num}: {e}")
        
        return None
    
    def _create_pdf_from_pages(self, pages: List[int], output_path: Path) -> bool:
        """إنشاء PDF من مجموعة صفحات"""
        try:
            # استخدام pdftk لاستخراج الصفحات
            pages_str = " ".join(str(p) for p in pages)
            cmd = f"pdftk {self.pdf_path} cat {pages_str} output {output_path}"
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            
            return output_path.exists() and output_path.stat().st_size > 0
        
        except Exception as e:
            logger.error(f"Failed to create PDF {output_path}: {e}")
            return False
    
    def _get_page_count(self) -> int:
        """الحصول على عدد صفحات PDF"""
        try:
            cmd = ['pdfinfo', str(self.pdf_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            for line in result.stdout.split('\n'):
                if line.startswith('Pages:'):
                    return int(line.split(':')[1].strip())
        except Exception as e:
            logger.error(f"Failed to get page count: {e}")
        
        return 0
    
    def _update_progress(self, progress: int, message: str):
        """تحديث تقدم المعالجة"""
        self.upload.update_progress(progress, message)
    
    def _sanitize_filename(self, filename: str) -> str:
        """تنظيف اسم الملف"""
        # إزالة الأحرف غير المسموح بها
        filename = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', filename)
        filename = re.sub(r'[\s]+', '_', filename)
        filename = filename.strip('._')
        
        # تقليل الطول إذا كان طويلاً جداً
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200 - len(ext)] + ext
        
        return filename or f"document_{int(time.time())}"
    
    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """استخراج تاريخ من النص"""
        patterns = [
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',  # YYYY-MM-DD
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',  # DD-MM-YYYY
            r'(\d{1,2}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1)
                try:
                    # محاولة تحويل التاريخ
                    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y'):
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            return dt.strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                except:
                    return date_str.replace('/', '-')
        
        return None
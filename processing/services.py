import os, re, hashlib, subprocess, logging, time
from pathlib import Path
from django.conf import settings
from .models import Upload, Group
from django.core.cache import cache
from django_redis import get_redis_connection

logger = logging.getLogger(__name__)

class BarcodeOCRService:
    def __init__(self):
        self.image_cache = {}
        self.barcode_cache = {}
        self.text_cache = {}
        self.ocr_cache = {}
        self.pdf_hash = None
        self.upload_id = None
        self.redis = get_redis_connection("default")

    def process_pdf(self, upload: Upload):
        lock_key = f"processing_{upload.id}"
        if self.redis.get(lock_key):
            logger.warning(f"Processing already in progress for upload {upload.id}")
            return []
        self.redis.set(lock_key, "true", ex=7200)
        self.upload_id = upload.id

        pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        # حذف المجموعات السابقة
        Group.objects.filter(upload=upload).delete()
        self.update_progress(upload.id, 0, "جاري تهيئة الملف...")

        self.pdf_hash = hashlib.md5(str(pdf_path).encode()).hexdigest()
        page_count = self.get_pdf_page_count(pdf_path)

        separator_barcode = self.read_page_barcode(pdf_path, 1) or "default_barcode"
        self.update_progress(upload.id, 5, "جاري تقسيم الصفحات إلى أقسام...")

        # تقسيم الصفحات إلى أقسام
        sections, current_section = [], []
        for page in range(1, page_count + 1):
            barcode = self.read_page_barcode(pdf_path, page)
            if barcode == separator_barcode:
                if current_section:
                    sections.append(current_section)
                current_section = []
            else:
                current_section.append(page)
        if current_section:
            sections.append(current_section)

        self.update_progress(upload.id, 10, "جاري إنشاء ملفات PDF للمجموعات...")
        created_groups = []
        total_sections = len(sections)

        for idx, pages in enumerate(sections):
            if not pages: continue
            progress_value = 10 + (idx / total_sections) * 85
            self.update_progress(upload.id, progress_value,
                                 f"جاري إنشاء المجموعة {idx + 1} من {total_sections}...")

            filename = self.generate_filename_with_ocr(pdf_path, pages, idx, separator_barcode)
            filename_safe = f"{filename}.pdf"
            directory = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
            directory.mkdir(parents=True, exist_ok=True)
            output_path = directory / filename_safe
            db_path = f"groups/{filename_safe}"

            if self.create_pdf(pdf_path, pages, output_path):
                group = Group.objects.create(
                    code=separator_barcode,
                    pdf_path=db_path,
                    pages_count=len(pages),
                    user=upload.user,
                    upload=upload
                )
                created_groups.append(group)
            else:
                logger.warning(f"Failed creating PDF for group {filename_safe}")

        self.update_progress(upload.id, 100, "تم الانتهاء من المعالجة")
        self.redis.delete(lock_key)
        upload.status = 'completed'
        upload.save()
        return created_groups

    def update_progress(self, upload_id, progress, message=""):
        try:
            cache.set(f"upload_progress:{upload_id}", progress, timeout=3600)
            cache.set(f"upload_message:{upload_id}", message, timeout=3600)
        except Exception as e:
            logger.warning(f"Failed to update progress: {e}")

    # توليد اسم الملف بناءً على OCR
    def generate_filename_with_ocr(self, pdf_path, pages, index, barcode):
        first_page = pages[0]
        content = self.extract_text_pdftotext(pdf_path, first_page)
        if not content or len(content) < 40:
            content = self.extract_text_ocr(pdf_path, first_page)

        saned_number = self.find_document_number(content, r'سند\s*[:\-]?\s*(\d{2,})')
        if saned_number: return self.sanitize_filename(saned_number)

        qeed_number = self.find_document_number(content, r'قيد\s*[:\-]?\s*(\d+)')
        if qeed_number: return self.sanitize_filename(qeed_number)

        date = self.find_date(content)
        if date: return self.sanitize_filename(date)

        return self.sanitize_filename(f"{barcode}_{index+1}")

    # استخراج النص من PDF بدون OCR
    def extract_text_pdftotext(self, pdf_path, page):
        cache_key = f"{self.pdf_hash}::pdftotext::{page}"
        if cache_key in self.text_cache: return self.text_cache[cache_key]

        temp_file = Path(settings.TEMP_ROOT) / f"pdftxt_{cache_key}.txt"
        temp_file.parent.mkdir(parents=True, exist_ok=True)
        cmd = f"pdftotext -f {page} -l {page} -layout {pdf_path} {temp_file}"
        subprocess.run(cmd, shell=True)
        content = temp_file.read_text(encoding='utf-8') if temp_file.exists() else ""
        content = re.sub(r'\s+', ' ', content).strip()
        self.text_cache[cache_key] = content
        return content

    # استخراج النص باستخدام OCR
    def extract_text_ocr(self, pdf_path, page):
        cache_key = f"{self.pdf_hash}::ocr::{page}"
        if cache_key in self.ocr_cache: return self.ocr_cache[cache_key]

        image_path = self.convert_to_image(pdf_path, page)
        if not image_path: return ""

        output_file = Path(settings.TEMP_ROOT) / f"ocr_{cache_key}"
        cmd = f"tesseract {image_path} {output_file} -l ara --psm 6"
        subprocess.run(cmd, shell=True)
        text_file = output_file.with_suffix('.txt')
        content = text_file.read_text(encoding='utf-8') if text_file.exists() else ""
        content = re.sub(r'\s+', ' ', content).strip()
        self.ocr_cache[cache_key] = content
        return content

    # تحويل الصفحة لصورة
    def convert_to_image(self, pdf_path, page):
        cache_key = f"{self.pdf_hash}::page::{page}"
        if cache_key in self.image_cache: return self.image_cache[cache_key]

        temp_dir = Path(settings.TEMP_ROOT)
        temp_dir.mkdir(parents=True, exist_ok=True)
        png_path = temp_dir / f"page_{cache_key}.png"
        cmd = f"pdftoppm -f {page} -l {page} -png -singlefile {pdf_path} {temp_dir}/page_{cache_key}"
        subprocess.run(cmd, shell=True)
        if png_path.exists():
            self.image_cache[cache_key] = png_path
            return png_path
        return None

    # قراءة الباركود من الصفحة
    def read_page_barcode(self, pdf_path, page):
        cache_key = f"{self.pdf_hash}::barcode::{page}"
        if cache_key in self.barcode_cache: return self.barcode_cache[cache_key]

        image_path = self.convert_to_image(pdf_path, page)
        if not image_path: return None
        cmd = f"zbarimg -q --raw {image_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        barcode = result.stdout.strip() if result.returncode == 0 else None
        self.barcode_cache[cache_key] = barcode
        return barcode

    # إنشاء PDF جديد من الصفحات
    def create_pdf(self, pdf_path, pages, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pages_str = " ".join(map(str, pages))
        cmd = f"pdftk {pdf_path} cat {pages_str} output {output_path}"
        subprocess.run(cmd, shell=True)
        return output_path.exists() and output_path.stat().st_size > 10000

    def sanitize_filename(self, filename):
        clean = re.sub(r'[^\w\-_\.]', '_', filename)
        clean = re.sub(r'[_\.]{2,}', '_', clean)
        return clean or f"file_{int(time.time())}"

    def get_pdf_page_count(self, pdf_path):
        cmd = f"pdfinfo {pdf_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        for line in result.stdout.splitlines():
            m = re.match(r'Pages:\s*(\d+)', line)
            if m: return int(m.group(1))
        raise Exception("Unable to determine page count")

    def find_document_number(self, content, pattern):
        m = re.search(pattern, content)
        return m.group(1) if m else None

    def find_date(self, content):
        patterns = [r'(\d{2}/\d{2}/\d{4})', r'(\d{2}-\d{2}-\d{4})', r'(\d{4}-\d{2}-\d{2})']
        for pat in patterns:
            m = re.search(pat, content)
            if m: return m.group(1).replace('/', '-')
        return None

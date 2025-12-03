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
        upload_id = upload.id
        logger.info(f"Start processing upload {upload_id}")

        pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        # حذف أي مجموعات سابقة
        Group.objects.filter(upload=upload).delete()

        # تحويل كل الصفحات إلى صور دفعة واحدة
        images = convert_from_path(str(pdf_path), dpi=150, thread_count=4)
        page_count = len(images)

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

        cache.set(f"upload_progress:{upload_id}", 50, timeout=3600)
        cache.set(f"upload_message:{upload_id}", f"{len(sections)} sections found...", timeout=3600)

        # إنشاء ملفات PDF لكل مجموعة
        created_groups = []
        for idx, pages in enumerate(sections):
            if not pages:
                continue
            filename = self.generate_filename(images, pages[0], separator_barcode, idx)
            filename_safe = f"{filename}.pdf"
            output_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / filename_safe

            # إنشاء PDF جديد لكل مجموعة باستخدام PyMuPDF
            import fitz
            new_doc = fitz.open()
            import fitz
            orig_doc = fitz.open(pdf_path)
            for p in pages:
                new_doc.insert_pdf(orig_doc, from_page=p, to_page=p)
            new_doc.save(output_path)
            new_doc.close()

            group = Group.objects.create(
                code=separator_barcode,
                pdf_path=f"groups/{filename_safe}",
                pages_count=len(pages),
                user=upload.user,
                upload=upload,
                filename=filename_safe
            )
            created_groups.append(group)

            # تحديث الـ progress
            progress = 50 + int((idx + 1) / len(sections) * 50)
            cache.set(f"upload_progress:{upload_id}", progress, timeout=3600)
            cache.set(f"upload_message:{upload_id}", f"Group {idx+1}/{len(sections)} created", timeout=3600)

        cache.set(f"upload_progress:{upload_id}", 100, timeout=3600)
        cache.set(f"upload_message:{upload_id}", "Processing complete", timeout=3600)
        return created_groups

    # -----------------------------
    # دوال مساعدة
    # -----------------------------
    def read_barcode_from_image(self, img):
        if img in self.barcode_cache:
            return self.barcode_cache[img]
        barcode = None
        for obj in decode_barcode(img):
            barcode = obj.data.decode("utf-8")
            break
        self.barcode_cache[img] = barcode
        return barcode

    def extract_text_from_image(self, img):
        if img in self.ocr_cache:
            return self.ocr_cache[img]
        text = pytesseract.image_to_string(img, lang="ara").strip()
        self.ocr_cache[img] = text
        return text

    def generate_filename(self, images, page_number, barcode, idx):
        text = self.extract_text_from_image(images[page_number])
        for pattern in [r'سند\s*[:\-]?\s*(\d+)', r'قيد\s*[:\-]?\s*(\d+)', r'(\d{4}-\d{2}-\d{2})']:
            m = re.search(pattern, text)
            if m:
                return self.sanitize_filename(m.group(1))
        return self.sanitize_filename(f"{barcode}_{idx+1}")

    def sanitize_filename(self, filename):
        filename = re.sub(r'[^\w\-_\.]', '_', filename)
        return filename or f"file_{os.getpid()}"

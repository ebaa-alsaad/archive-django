import os
import re
import hashlib
import logging
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import fitz  # PyMuPDF
from pdf2image import convert_from_path
import pytesseract
from pyzbar.pyzbar import decode as decode_barcode
from django.core.cache import cache
from .models import Upload, Group
from django.conf import settings

logger = logging.getLogger(__name__)

class BarcodeOCRService:
    """
    خدمة معالجة PDF:
    - تقسيم صفحات حسب الباركود الفاصل
    - استخراج النص باستخدام pdftotext أو OCR
    - توليد اسماء ملفات آمنة
    - إنشاء ملفات PDF جديدة لكل مجموعة
    - دعم معالجة عدة ملفات في نفس الريكوست
    """

    def __init__(self):
        self.text_cache = {}
        self.ocr_cache = {}
        self.image_cache = {}
        self.barcode_cache = {}

    def process_pdfs(self, uploads):
        """
        معالجة عدة ملفات PDF في نفس الوقت (قائمة Upload)
        """
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.process_single_pdf, upload): upload for upload in uploads}
            for future in futures:
                try:
                    res = future.result()
                    results.append((futures[future], res))
                except Exception as e:
                    logger.exception(f"Error processing upload {futures[future].id}: {e}")
                    results.append((futures[future], []))
        return results

    def process_single_pdf(self, upload):
        """
        معالجة ملف PDF واحد
        """
        upload_id = upload.id
        logger.info(f"Start processing upload {upload_id}")
        cache.set(f"upload_progress:{upload_id}", 0, timeout=3600)
        cache.set(f"upload_message:{upload_id}", "Starting...", timeout=3600)

        pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        # حذف أي مجموعات سابقة
        Group.objects.filter(upload=upload).delete()

        # hash للملف الحالي
        pdf_hash = hashlib.md5(str(pdf_path).encode()).hexdigest()
        doc = fitz.open(pdf_path)
        page_count = doc.page_count

        # الباركود الفاصل من الصفحة الأولى
        separator_barcode = self.read_barcode(doc, 0) or "default_barcode"

        # تقسيم الصفحات حسب الباركود
        sections, current_section = [], []
        for i in range(page_count):
            barcode = self.read_barcode(doc, i)
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
            filename = self.generate_filename(doc, pages[0], separator_barcode, idx)
            filename_safe = f"{filename}.pdf"

            output_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / filename_safe

            # إنشاء PDF جديد لكل مجموعة
            self.create_pdf(doc, pages, output_path)

            group = Group.objects.create(
                code=separator_barcode,
                pdf_path=f"groups/{filename_safe}",
                pages_count=len(pages),
                user=upload.user,
                upload=upload,
                filename=filename_safe
            )
            created_groups.append(group)
            progress = 50 + int((idx + 1) / len(sections) * 50)
            cache.set(f"upload_progress:{upload_id}", progress, timeout=3600)
            cache.set(f"upload_message:{upload_id}", f"Group {idx+1}/{len(sections)} created", timeout=3600)

        cache.set(f"upload_progress:{upload_id}", 100, timeout=3600)
        cache.set(f"upload_message:{upload_id}", "Processing complete", timeout=3600)
        return created_groups

    # -----------------------------
    # دوال مساعدة
    # -----------------------------
    def read_barcode(self, doc, page_number):
        """
        قراءة الباركود من صفحة PDF باستخدام pyzbar
        """
        if page_number in self.barcode_cache:
            return self.barcode_cache[page_number]

        # تحويل الصفحة إلى صورة
        img = self.pdf_page_to_image(doc, page_number)
        barcode = None
        for obj in decode_barcode(img):
            barcode = obj.data.decode("utf-8")
            break
        self.barcode_cache[page_number] = barcode
        return barcode

    def pdf_page_to_image(self, doc, page_number):
        """
        تحويل صفحة PDF إلى صورة باستخدام PyMuPDF + PIL
        """
        if page_number in self.image_cache:
            return self.image_cache[page_number]

        page = doc.load_page(page_number)
        pix = page.get_pixmap(dpi=300)
        img_path = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
        pix.save(img_path)
        from PIL import Image
        img = Image.open(img_path)
        self.image_cache[page_number] = img
        return img

    def extract_text(self, doc, page_number):
        """
        استخراج النص من PDF (pdf + OCR fallback)
        """
        if page_number in self.text_cache:
            return self.text_cache[page_number]

        page = doc.load_page(page_number)
        text = page.get_text("text").strip()
        if not text or len(text) < 40:
            img = self.pdf_page_to_image(doc, page_number)
            text = pytesseract.image_to_string(img, lang="ara").strip()
        self.text_cache[page_number] = text
        return text

    def generate_filename(self, doc, page_number, barcode, idx):
        """
        توليد اسم الملف بناءً على OCR أو fallback
        """
        content = self.extract_text(doc, page_number)

        # البحث عن رقم السند أو القيد أو التاريخ
        for pattern in [r'سند\s*[:\-]?\s*(\d+)', r'قيد\s*[:\-]?\s*(\d+)', r'(\d{4}-\d{2}-\d{2})']:
            m = re.search(pattern, content)
            if m:
                return self.sanitize_filename(m.group(1))
        return self.sanitize_filename(f"{barcode}_{idx+1}")

    def sanitize_filename(self, filename):
        """
        تنظيف اسم الملف ليكون صالح
        """
        filename = re.sub(r'[^\w\-_\.]', '_', filename)
        return filename or f"file_{os.getpid()}"

    def create_pdf(self, doc, pages, output_path):
        """
        إنشاء PDF جديد من صفحات محددة
        """
        new_doc = fitz.open()
        for p in pages:
            new_doc.insert_pdf(doc, from_page=p, to_page=p)
        new_doc.save(output_path)
        new_doc.close()

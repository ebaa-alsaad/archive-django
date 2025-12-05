# services/barcode_service.py
import os
import re
import logging
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import fitz  # PyMuPDF
from pyzbar.pyzbar import decode as decode_barcode
import cv2
from django.core.cache import cache
from django.conf import settings
from .models import Upload, Group
import threading
import time

logger = logging.getLogger(__name__)

class BarcodeOCRService:
    """
    محسّن لاستخراج باركود وتقسيم PDF إلى مجموعات.
    - إجبار RGB عبر fitz.csRGB
    - DPI أعلى (200)
    - fallback إلى zbarimg (مستقل عن بايثون) إذا pyzbar فشل
    - استبعاد صفحات الباركود من المجموعات
    - يعمل متوافقاً مع معالجة بالخلفية (Celery)
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.current_upload = None
        self.MAX_WORKERS = min(4, (os.cpu_count() or 2))
        self.DPI = 200

    # ---------------------------
    # عمليّة قراءة باركود من صورة باستخدام zbarimg (fallback)
    # ---------------------------
    def _read_barcode_with_zbarimg(self, img_path):
        try:
            out = subprocess.check_output(['zbarimg', '--raw', str(img_path)], stderr=subprocess.DEVNULL, timeout=6)
            text = out.decode('utf-8', errors='ignore').strip()
            if text:
                return text
        except Exception:
            return None

    # ---------------------------
    # استخراج باركود من صفحة PDF
    # ---------------------------
    def _extract_barcode_from_pdf_page(self, doc, page_num):
        try:
            page = doc[page_num]

            # 1) حاول استخراج النص أولاً (أسرع)
            text = page.get_text("text")
            if text and len(text.strip()) > 6:
                # أنماط معقولة للباركود
                patterns = [
                    r'\b\d{8,20}\b',
                    r'باركود[\s:]*(\d+)',
                    r'Barcode[\s:]*(\d+)',
                    r'Code[\s:]*(\d+)',
                    r'رقم[\s:]*(\d+)',
                ]
                for pat in patterns:
                    matches = re.findall(pat, text, flags=re.IGNORECASE)
                    if matches:
                        cand = str(matches[0]).strip()
                        if len(cand) >= 3:
                            return cand

            # 2) صورة: إجبار RGB لإخراج ثابت من PyMuPDF
            pix = page.get_pixmap(dpi=self.DPI, colorspace=fitz.csRGB)
            if not pix or not getattr(pix, 'samples', None):
                return None

            # بناء مصفوفة numpy
            arr = np.frombuffer(pix.samples, dtype=np.uint8)
            n_channels = pix.n
            try:
                img = arr.reshape(pix.height, pix.width, n_channels)
            except Exception:
                # في حال تعذر إعادة التحجيم
                return None

            # تحويل إلى رمادي
            if img.ndim == 3:
                gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
            else:
                gray = img

            # تحسين الصورة للباركود:عتبة أوتسو
            try:
                gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
            except Exception:
                pass

            # 3) محاولة decode بواسطة pyzbar
            try:
                decoded = decode_barcode(gray)
                if decoded:
                    for d in decoded:
                        txt = d.data.decode('utf-8', errors='ignore').strip()
                        if txt:
                            return txt
            except Exception:
                pass

            # 4) fallback: اكتب صورة مؤقتة واستخدم zbarimg
            tmp_dir = Path(settings.PRIVATE_MEDIA_ROOT) / "tmp_bcode"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_img = tmp_dir / f"page_{page_num}_{int(time.time()*1000)}.png"
            try:
                # حفظ PNG باستخدام PIL عبر cv2
                cv2.imwrite(str(tmp_img), gray)
                zres = self._read_barcode_with_zbarimg(tmp_img)
                try:
                    tmp_img.unlink()
                except Exception:
                    pass
                if zres:
                    return zres
            except Exception:
                pass

            return None

        except Exception as e:
            logger.debug(f"فشل استخراج الباركود من الصفحة {page_num}: {e}")
            return None

    # ---------------------------
    # ايجاد الباركود الفاصل
    # ---------------------------
    def _find_separator_barcode_fast(self, doc):
        total_pages = doc.page_count
        # صفحات مرجعية: 0, 1..5, mid, last
        check_pages = [0]
        for i in range(1, min(6, total_pages)):
            check_pages.append(i)
        if total_pages > 10:
            check_pages.append(total_pages // 2)
        if total_pages > 1:
            check_pages.append(total_pages - 1)
        seen = set()
        for p in check_pages:
            if p in seen: 
                continue
            seen.add(p)
            code = self._extract_barcode_from_pdf_page(doc, p)
            if code and str(code).strip():
                logger.info(f"وجد باركود فاصل في صفحة {p}: {code}")
                return str(code).strip()
        # افتراضي: اسم الملف بدون امتداد (قص 40 حرف)
        default_code = Path(doc.name).stem[:40] or "document"
        logger.info(f"استخدام باركود افتراضي: {default_code}")
        return default_code

    # ---------------------------
    # تقسيم صفحات الملف إلى أقسام (لا تضيف صفحة الباركود نفسها)
    # ---------------------------
    def _split_pages(self, doc, separator_barcode):
        total = doc.page_count
        sections = []
        current = []
        for i in range(total):
            code = self._extract_barcode_from_pdf_page(doc, i)
            code_str = str(code).strip() if code else ""
            if code_str and code_str == str(separator_barcode).strip():
                # صفحة باركود: ابدأ قسم جديد (ولا تضيف صفحة الباركود)
                if current:
                    sections.append(current.copy())
                    current = []
                else:
                    # إذا كان current فارغًا: هذا يعني باركود متتالي؛ تجاهل
                    current = []
            else:
                current.append(i)
        if current:
            sections.append(current.copy())
        return sections

    # ---------------------------
    # إنشاء المجموعات وملفات PDF على القرص وإدخال DB
    # ---------------------------
    def _create_groups(self, doc, sections, separator_barcode, upload):
        created = []
        outdir = Path(settings.PRIVATE_MEDIA_ROOT) / "groups"
        outdir.mkdir(parents=True, exist_ok=True)

        def make_group(idx, pages):
            if not pages:
                return None
            # اسم المجموعة من أول صفحة
            name = None
            try:
                ptext = doc[pages[0]].get_text("text")
                name = None
                if ptext and len(ptext.strip()) > 6:
                    # أنماط الاسم مثل سند أو قيد أو تاريخ
                    patterns = [
                        r'رقم\s*السند\s*[:\-]?\s*(\d{2,})',
                        r'السند\s*[:\-]?\s*(\d{2,})',
                        r'قيد\s*[:\-]?\s*(\d+)',
                        r'(\d{4}-\d{2}-\d{2})',
                    ]
                    for pat in patterns:
                        m = re.findall(pat, ptext, flags=re.IGNORECASE)
                        if m:
                            name = m[0]
                            break
            except Exception:
                pass

            if not name:
                name = f"{separator_barcode}_{idx+1}"

            # sanitize
            filename = re.sub(r'[^\w\-_\.]', '_', name)[:75] + ".pdf"
            outpath = outdir / filename

            newdoc = fitz.open()
            for p in pages:
                newdoc.insert_pdf(doc, from_page=p, to_page=p)
            newdoc.save(outpath, deflate=True, garbage=4, clean=True)
            newdoc.close()

            # file size check
            if not outpath.exists() or outpath.stat().st_size < 5*1024:
                logger.warning(f"ملف صغير جدًا: {outpath}")
                return None

            group = Group.objects.create(
                code=separator_barcode,
                pdf_path=f"groups/{filename}",
                pages_count=len(pages),
                user=upload.user,
                upload=upload,
                filename=filename,
                name=filename.rsplit('.', 1)[0]
            )
            return group

        # parallel creation
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as ex:
            futures = {ex.submit(make_group, idx, pages): idx for idx, pages in enumerate(sections)}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res:
                        created.append(res)
                except Exception as e:
                    logger.error(f"خطأ في إنشاء مجموعة: {e}")

        return created

    # ---------------------------
    # دالة المعالجة الوحيدة
    # ---------------------------
    def process_single_pdf(self, upload):
        self.current_upload = upload
        upload_id = upload.id
        start = time.time()
        logger.info(f"بدء معالجة upload {upload_id}")

        try:
            pdf_path = Path(settings.PRIVATE_MEDIA_ROOT) / upload.stored_filename
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF not found: {pdf_path}")

            with self._lock:
                upload.status = 'processing'
                upload.progress = 5
                upload.message = 'J: تهيئة الملف'
                upload.save(update_fields=['status', 'progress', 'message'])

            doc = fitz.open(pdf_path)
            total_pages = doc.page_count
            logger.info(f"عدد الصفحات: {total_pages}")

            separator = self._find_separator_barcode_fast(doc)
            logger.info(f"باركود الفاصل: {separator}")

            self._update_progress(upload, 20, 'جاري تقسيم الصفحات...')
            sections = self._split_pages(doc, separator)
            if not sections:
                # لا أقسام → نُنشئ مجموعة واحدة من كل الصفحات مع استبعاد حالات الباركود إن كانت موجودة
                sections = []
                # إذا كان separator يطابق صفحة ما، استبعدها — لكن منطقيًّا separator قد يكون اسم الملف وليس باركود فعلي
                # هنا نأخذ كل الصفحات كقسم واحد
                sections.append([i for i in range(total_pages)])

            logger.info(f"الأقسام المكتشفة: {len(sections)}")
            Group.objects.filter(upload=upload).delete()
            self._update_progress(upload, 50, 'جاري إنشاء ملفات المجموعات...')
            created = self._create_groups(doc, sections, separator, upload)

            doc.close()

            elapsed = time.time() - start
            with self._lock:
                upload.status = 'completed'
                upload.progress = 100
                upload.message = f'اكتملت في {elapsed:.1f}s. مجموعات: {len(created)}'
                upload.save(update_fields=['status', 'progress', 'message'])

            logger.info(f"انتهت المعالجة: upload {upload_id} مجموعات {len(created)}")
            return created

        except Exception as e:
            logger.exception(f"فشل المعالجة upload {upload_id}: {e}")
            with self._lock:
                upload.status = 'failed'
                upload.message = f'خطأ: {str(e)[:200]}'
                upload.save(update_fields=['status', 'message'])
            raise

    # ---------------------------
    def _update_progress(self, upload, progress, message=''):
        try:
            with self._lock:
                upload.progress = int(progress)
                upload.message = message
                upload.save(update_fields=['progress', 'message'])
                cache.set(f"upload_progress_{upload.id}", {'progress': progress, 'message': message, 'ts': time.time()}, 30)
        except Exception as e:
            logger.warning(f"فشل تحديث التقدم: {e}")

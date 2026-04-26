import hashlib
import io
import os
import sys

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


_ORIGINAL_MD5 = hashlib.md5
_FONT_REGISTERED = False


def _md5_safe(*args, **kwargs):
    # Some Python/OpenSSL builds don't accept `usedforsecurity` in md5().
    kwargs.pop("usedforsecurity", None)
    return _ORIGINAL_MD5(*args, **kwargs)


def _apply_md5_compatibility_patch() -> None:
    hashlib.md5 = _md5_safe
    for _name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        existing = getattr(mod, "md5", None)
        if existing is _ORIGINAL_MD5:
            try:
                mod.md5 = _md5_safe
            except Exception:
                pass


_apply_md5_compatibility_patch()

from xhtml2pdf import pisa  # noqa: E402  imported after md5 patch is in place


def _register_fonts() -> None:
    global _FONT_REGISTERED
    if _FONT_REGISTERED:
        return

    regular_font_path = os.path.join(os.getcwd(), "template", "fonts", "SFArabic.ttf")
    bold_font_path = os.path.join(os.getcwd(), "template", "fonts", "SFArabic.ttf")
    symbols_font_path = os.path.join(os.getcwd(), "template", "fonts", "ArialUnicode.ttf")
    if os.path.exists(regular_font_path):
        pdfmetrics.registerFont(TTFont("ReportArabic", regular_font_path))
    if os.path.exists(bold_font_path):
        pdfmetrics.registerFont(TTFont("ReportArabicBold", bold_font_path))
    if os.path.exists(symbols_font_path):
        pdfmetrics.registerFont(TTFont("ReportSymbols", symbols_font_path))
    if os.path.exists(regular_font_path) and os.path.exists(bold_font_path):
        pdfmetrics.registerFontFamily(
            "ReportArabic",
            normal="ReportArabic",
            bold="ReportArabicBold",
            italic="ReportArabic",
            boldItalic="ReportArabicBold",
        )
    _FONT_REGISTERED = True


def _link_callback(uri: str, _rel: str) -> str:
    if uri.startswith(("http://", "https://", "data:")):
        return uri
    project_root = os.getcwd()
    candidate = os.path.join(project_root, uri.lstrip("/"))
    return candidate if os.path.exists(candidate) else uri


def html_to_pdf_bytes(html_content: str) -> bytes:
    _apply_md5_compatibility_patch()
    _register_fonts()
    pdf_buffer = io.BytesIO()
    result = pisa.CreatePDF(
        src=html_content,
        dest=pdf_buffer,
        link_callback=_link_callback,
    )
    if result.err:
        raise ValueError("Failed to generate PDF from HTML.")
    return pdf_buffer.getvalue()

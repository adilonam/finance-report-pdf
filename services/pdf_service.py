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

    regular_font_path = os.path.join(
        os.getcwd(),
        "template",
        "fonts",
        "Noto_Kufi_Arabic",
        "static",
        "NotoKufiArabic-Regular.ttf",
    )
    bold_font_path = os.path.join(
        os.getcwd(),
        "template",
        "fonts",
        "Noto_Kufi_Arabic",
        "static",
        "NotoKufiArabic-Bold.ttf",
    )
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


def _resolve_local_uri(uri: str, base_dir: str) -> str:
    if os.path.isabs(uri):
        return uri if os.path.exists(uri) else uri

    project_root = os.getcwd()
    root_relative_uri = uri.lstrip("/")
    base_relative_uri = root_relative_uri[2:] if root_relative_uri.startswith("./") else root_relative_uri
    candidates = [
        os.path.join(base_dir, base_relative_uri),
        os.path.join(project_root, root_relative_uri),
        os.path.join(project_root, base_relative_uri),
    ]

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return uri


def _make_link_callback(base_dir: str):
    def _link_callback(uri: str, _rel: str) -> str:
        if uri.startswith(("http://", "https://", "data:")):
            return uri
        return _resolve_local_uri(uri, base_dir)

    return _link_callback


def _link_callback(uri: str, _rel: str) -> str:
    if uri.startswith(("http://", "https://", "data:")):
        return uri
    return _resolve_local_uri(uri, os.getcwd())


def html_to_pdf_bytes(html_content: str, base_dir: str = "template") -> bytes:
    _apply_md5_compatibility_patch()
    _register_fonts()
    absolute_base_dir = os.path.abspath(base_dir)
    pdf_buffer = io.BytesIO()
    result = pisa.CreatePDF(
        src=html_content,
        dest=pdf_buffer,
        link_callback=_make_link_callback(absolute_base_dir),
    )
    if result.err:
        raise ValueError("Failed to generate PDF from HTML.")
    return pdf_buffer.getvalue()

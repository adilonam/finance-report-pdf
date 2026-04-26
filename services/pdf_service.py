import hashlib
import io
import os
import tempfile
import sys
from pathlib import Path


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


def _register_fonts() -> None:
    global _FONT_REGISTERED
    if _FONT_REGISTERED:
        return

    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

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
    try:
        from xhtml2pdf import pisa

        _register_fonts()
    except ImportError as exc:
        raise ValueError("HTML PDF export requires xhtml2pdf and reportlab.") from exc

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


def browser_html_to_pdf_bytes(html_content: str, base_dir: str = "template") -> bytes:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ValueError(
            "Browser PDF export requires Playwright. Install dependencies, then run: "
            "playwright install chromium"
        ) from exc

    absolute_base_dir = Path(base_dir).resolve()
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".html",
        prefix="browser-report-",
        dir=absolute_base_dir,
        delete=False,
    )
    try:
        with temp_file:
            temp_file.write(html_content)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.goto(Path(temp_file.name).as_uri(), wait_until="networkidle")
            pdf_bytes = page.pdf(
                format="A4",
                print_background=True,
                prefer_css_page_size=True,
            )
            browser.close()
            return pdf_bytes
    except PlaywrightError as exc:
        raise ValueError(
            "Failed to generate PDF with Chromium. If this is the first run, execute: "
            "playwright install chromium"
        ) from exc
    finally:
        try:
            os.unlink(temp_file.name)
        except FileNotFoundError:
            pass

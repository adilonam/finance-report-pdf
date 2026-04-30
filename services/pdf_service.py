import subprocess
import tempfile
import sys
from pathlib import Path


def _install_playwright_chromium() -> None:
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _render_pdf_with_browser(html_file_path: Path) -> bytes:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            page = browser.new_page()
            page.goto(html_file_path.as_uri(), wait_until="networkidle")
            return page.pdf(
                width="297mm",
                height="210mm",
                print_background=True,
            )
        finally:
            browser.close()


def browser_html_to_pdf_bytes(html_content: str, base_dir: str = "template") -> bytes:
    try:
        from playwright.sync_api import Error as PlaywrightError
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

        html_file_path = Path(temp_file.name)
        try:
            return _render_pdf_with_browser(html_file_path)
        except PlaywrightError:
            _install_playwright_chromium()
            return _render_pdf_with_browser(html_file_path)
    except PlaywrightError as exc:
        raise ValueError(
            "Failed to generate PDF with Chromium after installing the browser runtime."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise ValueError(
            "Failed to install Chromium for Playwright. On Streamlit Cloud, make sure "
            "packages.txt is committed with the required Linux browser libraries."
        ) from exc
    finally:
        try:
            Path(temp_file.name).unlink()
        except FileNotFoundError:
            pass

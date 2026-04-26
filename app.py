import base64
import os

import streamlit as st
from services.mock_api_service import get_qse_daily_report_data
from services.pdf_service import browser_html_to_pdf_bytes
from services.template_service import render_html_template


st.set_page_config(page_title="HTML Report to PDF", layout="centered")
st.title("QSE Daily Report (Mock API)")
st.caption("HTML report source and browser PDF export.")


def show_pdf_open_link(pdf_bytes: bytes) -> None:
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    st.markdown(
        f'<a href="data:application/pdf;base64,{pdf_b64}" target="_blank" '
        'rel="noopener noreferrer">Open PDF in new tab</a>',
        unsafe_allow_html=True,
    )

if st.button("Refresh mocked API data"):
    st.rerun()

report_data = get_qse_daily_report_data()

template_path = "template/report_template.html"

try:
    preview_html = render_html_template(template_path, report_data, shape_arabic=False)
except FileNotFoundError:
    st.error("Template file not found at template/report_template.html")
    st.stop()

with st.expander("HTML Code", expanded=False):
    st.code(preview_html, language="html")

if st.button("Generate PDF with Browser"):
    try:
        pdf_bytes = browser_html_to_pdf_bytes(preview_html, base_dir=os.path.dirname(template_path))
        st.success("PDF generated.")
        show_pdf_open_link(pdf_bytes)
        st.download_button(
            "Download Browser PDF",
            data=pdf_bytes,
            file_name="qse-daily-report-browser.pdf",
            mime="application/pdf",
        )
    except ValueError as error:
        st.error(str(error))

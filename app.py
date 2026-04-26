import base64
import os

import streamlit as st
from services.mock_api_service import get_qse_daily_report_data
from services.pdf_service import html_to_pdf_bytes
from services.template_service import render_html_template


st.set_page_config(page_title="HTML Report to PDF", layout="centered")
st.title("QSE Daily Report (Mock API)")
st.caption("HTML report source and PDF preview.")


def show_pdf_preview(pdf_bytes: bytes, title: str) -> None:
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    pdf_viewer = (
        f'<iframe src="data:application/pdf;base64,{pdf_b64}" '
        'width="100%" height="900" type="application/pdf"></iframe>'
    )
    st.subheader(title)
    st.markdown(pdf_viewer, unsafe_allow_html=True)

if st.button("Refresh mocked API data"):
    st.rerun()

report_data = get_qse_daily_report_data()

template_path = "template/report_template.html"

try:
    preview_html = render_html_template(template_path, report_data, shape_arabic=False)
    pdf_html = render_html_template(template_path, report_data)
except FileNotFoundError:
    st.error("Template file not found at template/report_template.html")
    st.stop()

with st.expander("HTML Code", expanded=False):
    st.code(preview_html, language="html")

if st.button("Preview PDF"):
    try:
        pdf_bytes = html_to_pdf_bytes(pdf_html, base_dir=os.path.dirname(template_path))
        show_pdf_preview(pdf_bytes, "PDF Preview")
    except ValueError as error:
        st.error(str(error))

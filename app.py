from pathlib import Path

import streamlit as st
from services.page1_service import run_update_jobs
from services.report_data_service import get_qse_daily_report_data
from services.pdf_service import browser_html_to_pdf_bytes
from services.template_service import render_html_template
from ui.pdf_controls import show_pdf_actions


st.set_page_config(page_title="HTML Report to PDF", layout="centered")
st.title("QSE Daily Report")
st.caption("HTML report source and browser PDF export.")

TEMPLATE_PATH = Path("template/report_template.html")

if st.button("Run Job"):
    try:
        with st.spinner("Updating local database..."):
            result = run_update_jobs()
        st.session_state["last_job_result"] = result
    except Exception as error:
        st.error(f"Job failed: {error}")

if "last_job_result" in st.session_state:
    result = st.session_state["last_job_result"]
    with st.expander("Job logs", expanded=False):
        st.code(
            "\n".join(result["log_lines"]),
            language="text",
        )

try:
    preview_html = render_html_template(str(TEMPLATE_PATH), get_qse_daily_report_data())
except FileNotFoundError:
    st.error("Template file not found at template/report_template.html")
    st.stop()

with st.expander("HTML Code", expanded=False):
    st.code(preview_html, language="html")

if st.button("Generate PDF with Browser"):
    try:
        pdf_bytes = browser_html_to_pdf_bytes(preview_html, base_dir=str(TEMPLATE_PATH.parent))
        st.success("PDF generated.")
        show_pdf_actions(pdf_bytes, file_name="qse-daily-report-browser.pdf")
    except ValueError as error:
        st.error(str(error))

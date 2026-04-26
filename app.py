import base64

import streamlit as st
from services.mock_api_service import get_qse_daily_report_data
from services.pdf_service import html_to_pdf_bytes
from services.template_service import render_html_template


st.set_page_config(page_title="HTML Report to PDF", layout="centered")
st.title("QSE Daily Report (Mock API)")
st.caption("Template aligned to your sample report design, powered by mocked APIs.")

if st.button("Refresh mocked API data"):
    st.rerun()

report_data = get_qse_daily_report_data()

template_path = "template/report_template.html"

try:
    rendered_html = render_html_template(template_path, report_data)
except FileNotFoundError:
    st.error("Template file not found at template/report_template.html")
    st.stop()

st.subheader("Report Actions")
generate_pdf, preview_pdf = st.columns(2)

if generate_pdf.button("Generate PDF"):
    try:
        pdf_bytes = html_to_pdf_bytes(rendered_html)
        st.success("PDF generated successfully.")
        st.download_button(
            label="Download report PDF",
            data=pdf_bytes,
            file_name="finance_report.pdf",
            mime="application/pdf",
        )
    except ValueError as error:
        st.error(str(error))

if preview_pdf.button("Preview PDF"):
    try:
        pdf_bytes = html_to_pdf_bytes(rendered_html)
        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
        pdf_viewer = (
            f'<iframe src="data:application/pdf;base64,{pdf_b64}" '
            'width="100%" height="900" type="application/pdf"></iframe>'
        )
        st.subheader("PDF Preview")
        st.markdown(pdf_viewer, unsafe_allow_html=True)
    except ValueError as error:
        st.error(str(error))

with st.expander("HTML Code", expanded=False):
    st.code(rendered_html, language="html")

import io
from datetime import date

import streamlit as st
from jinja2 import Template
from xhtml2pdf import pisa


def render_html_template(template_path: str, context: dict) -> str:
    with open(template_path, "r", encoding="utf-8") as file:
        template = Template(file.read())
    return template.render(**context)


def html_to_pdf_bytes(html_content: str) -> bytes:
    pdf_buffer = io.BytesIO()
    result = pisa.CreatePDF(src=html_content, dest=pdf_buffer)
    if result.err:
        raise ValueError("Failed to generate PDF from HTML.")
    return pdf_buffer.getvalue()


st.set_page_config(page_title="HTML Report to PDF", layout="centered")
st.title("Single Report PDF Generator")
st.caption("Render one HTML report template and export it as PDF.")

default_context = {
    "report_title": "Monthly Finance Report",
    "report_date": str(date.today()),
    "company_name": "Demo Finance Ltd.",
    "author_name": "Analyst Team",
    "summary_text": (
        "Revenue grew by 12% month-over-month while operating expenses "
        "increased by 4%, resulting in stronger net margins."
    ),
    "revenue": "€125,000",
    "expenses": "€73,000",
    "net_profit": "€52,000",
}

st.subheader("Report Values")
report_title = st.text_input("Report title", value=default_context["report_title"])
report_date = st.date_input("Report date", value=date.today())
company_name = st.text_input("Company name", value=default_context["company_name"])
author_name = st.text_input("Author name", value=default_context["author_name"])
summary_text = st.text_area("Summary", value=default_context["summary_text"], height=120)

col1, col2, col3 = st.columns(3)
with col1:
    revenue = st.text_input("Revenue", value=default_context["revenue"])
with col2:
    expenses = st.text_input("Expenses", value=default_context["expenses"])
with col3:
    net_profit = st.text_input("Net profit", value=default_context["net_profit"])

context = {
    "report_title": report_title,
    "report_date": str(report_date),
    "company_name": company_name,
    "author_name": author_name,
    "summary_text": summary_text,
    "revenue": revenue,
    "expenses": expenses,
    "net_profit": net_profit,
}

template_path = "template/report_template.html"

try:
    rendered_html = render_html_template(template_path, context)
except FileNotFoundError:
    st.error("Template file not found at template/report_template.html")
    st.stop()

st.subheader("HTML Preview")
st.code(rendered_html, language="html")

if st.button("Generate PDF"):
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

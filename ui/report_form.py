from datetime import date

import streamlit as st

from models.report import ReportData


def report_form(default_report: ReportData) -> ReportData:
    st.subheader("Report Values")
    report_title = st.text_input("Report title", value=default_report.report_title)
    report_date = st.date_input("Report date", value=date.today())
    company_name = st.text_input("Company name", value=default_report.company_name)
    author_name = st.text_input("Author name", value=default_report.author_name)
    summary_text = st.text_area("Summary", value=default_report.summary_text, height=120)

    col1, col2, col3 = st.columns(3)
    with col1:
        revenue = st.text_input("Revenue", value=default_report.revenue)
    with col2:
        expenses = st.text_input("Expenses", value=default_report.expenses)
    with col3:
        net_profit = st.text_input("Net profit", value=default_report.net_profit)

    return ReportData(
        report_title=report_title,
        report_date=str(report_date),
        company_name=company_name,
        author_name=author_name,
        summary_text=summary_text,
        revenue=revenue,
        expenses=expenses,
        net_profit=net_profit,
    )

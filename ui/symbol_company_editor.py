from __future__ import annotations

import streamlit as st

from services.api_data_service import (
    load_listed_companies,
    parse_symbol_company_lines,
    save_listed_companies,
)


def _default_text_from_db() -> str:
    rows = load_listed_companies()
    return "\n".join(f"{r['symbol']},{r['company_name']}" for r in rows)


def show_symbol_company_editor() -> None:
    st.subheader("الشركات المدرجة (رمز، اسم)")
    st.caption(
        "سطر لكل شركة: الرمز، فاصلة، الاسم. يُمسح الجدول في قاعدة البيانات ثم تُدرج القيم الجديدة."
    )
    default = _default_text_from_db()
    text = st.text_area(
        "شركات",
        value=default,
        height=220,
        placeholder="ORDS,Ooredoo\nDHBK,Doha B",
        label_visibility="collapsed",
        key="listed_companies_text_area",
    )
    if st.button("حفظ الرموز والأسماء"):
        parsed = parse_symbol_company_lines(text)
        count = save_listed_companies(parsed)
        if count:
            st.success(f"تم الحفظ: {count} صفًا في listed_companies.")
        else:
            st.warning("تم مسح الجدول (لا توجد أسطر صالحة).")
        st.rerun()

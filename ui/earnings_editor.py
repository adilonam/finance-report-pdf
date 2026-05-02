from __future__ import annotations

import streamlit as st

from services.api_data_service import (
    load_earnings_quarters,
    parse_earnings_quarter_lines,
    save_earnings_quarters,
)


def _default_text_from_db() -> str:
    rows = load_earnings_quarters()
    return "\n".join(f"{r['label']}, {r['price']}" for r in rows)


def show_earnings_editor() -> None:
    st.subheader("إفصاح الأرباح (إدخال يدوي)")
    st.caption(
        "سطر لكل فصل: التسمية، فاصلة، السعر. يُمسح الجدول في قاعدة البيانات ثم تُدرج القيم الجديدة. "
        "بدون بيانات لا يُعرض قسم الرسم في ملف PDF."
    )
    default = _default_text_from_db()
    text = st.text_area(
        "البيانات",
        value=default,
        height=220,
        placeholder="Q1 2024, 3200\nQ2 2024, 3230",
        label_visibility="collapsed",
    )
    if st.button("حفظ البيانات واستبدال الجدول"):
        parsed = parse_earnings_quarter_lines(text)
        count = save_earnings_quarters(parsed)
        if count:
            st.success(f"تم الحفظ: {count} صفًا في earnings_quarters.")
        else:
            st.warning("تم مسح الجدول (لا توجد أسطر صالحة). لن يظهر قسم الأرباح في PDF.")
        st.rerun()

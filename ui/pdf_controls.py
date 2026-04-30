import base64

import streamlit as st


def show_pdf_actions(pdf_bytes: bytes, file_name: str) -> None:
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    st.markdown(
        f'<a href="data:application/pdf;base64,{pdf_b64}" target="_blank" '
        'rel="noopener noreferrer">Open PDF in new tab</a>',
        unsafe_allow_html=True,
    )
    st.download_button(
        "Download Browser PDF",
        data=pdf_bytes,
        file_name=file_name,
        mime="application/pdf",
    )

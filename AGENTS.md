# AGENTS

This project is a simple Streamlit app that renders one HTML report template and exports a PDF.

## Guidelines

- Keep `app.py` as a thin orchestration layer only.
- Put rendering and PDF logic in `services/`.
- Put Streamlit form/UI pieces in `ui/`.
- Keep the HTML report template in `template/report_template.html`.
- Keep static report assets in `template/`.
- Prefer small, focused functions and avoid large monolithic files.
- i use conda env use conda activate finance-report-pdf

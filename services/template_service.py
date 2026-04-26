import re

import arabic_reshaper
from bidi.algorithm import get_display
from jinja2 import Template


ARABIC_RE = re.compile(r"[\u0600-\u06ff]")
ARABIC_RUN_RE = re.compile(r"[\u0600-\u06ff][\u0600-\u06ff\s\u0640،؛؟]*")
HTML_TAG_RE = re.compile(r"(<[^>]+>)")


def _shape_arabic_text(text: str) -> str:
    if not ARABIC_RE.search(text):
        return text

    def _shape_match(match: re.Match) -> str:
        value = match.group(0)
        arabic_chars = ARABIC_RE.findall(value)
        if len(arabic_chars) <= 1:
            return value
        reshaped = arabic_reshaper.reshape(value)
        return get_display(reshaped, base_dir="R")

    return ARABIC_RUN_RE.sub(_shape_match, text)


def _shape_arabic_html(html: str) -> str:
    parts = HTML_TAG_RE.split(html)
    return "".join(
        part if part.startswith("<") and part.endswith(">") else _shape_arabic_text(part)
        for part in parts
    )


def render_html_template(template_path: str, context: dict) -> str:
    with open(template_path, "r", encoding="utf-8") as file:
        template = Template(file.read())
    rendered_html = template.render(**context)
    return _shape_arabic_html(rendered_html)

from jinja2 import Template


def render_html_template(template_path: str, context: dict) -> str:
    with open(template_path, "r", encoding="utf-8") as file:
        template = Template(file.read())
    return template.render(**context)

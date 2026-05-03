from __future__ import annotations

import json
import logging
from typing import Literal

logger = logging.getLogger(__name__)

Kind = Literal["exchange", "company"]


def attach_ai_summaries_to_news_rows(
    rows: list[dict],
    *,
    api_key: str | None,
    kind: Kind,
) -> list[dict]:
    """Adds ``ai_headline`` and ``ai_summary`` (Arabic per row; may be empty)."""
    if not rows:
        return []

    out: list[dict] = []
    for r in rows:
        item = dict(r)
        item["ai_headline"] = ""
        item["ai_summary"] = ""
        out.append(item)

    if not api_key or not (api_key := api_key.strip()):
        return out

    pairs = _summarize_batch([dict(r) for r in rows], api_key=api_key, kind=kind)
    for index, (headline, summary) in enumerate(pairs):
        if index < len(out):
            out[index]["ai_headline"] = headline
            out[index]["ai_summary"] = summary
    return out


def _summarize_batch(rows: list[dict], *, api_key: str, kind: Kind) -> list[tuple[str, str]]:
    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed; skipping news summaries")
        return [("", "")] * len(rows)

    label_ar = "أخبار البورصة" if kind == "exchange" else "أخبار الشركات"
    payload_items = []
    for index, row in enumerate(rows):
        headline = str(row.get("headline", "")).strip()
        body = str(row.get("source_summary", "")).strip()
        payload_items.append({"i": index + 1, "headline": headline, "summary": body})

    system = (
        "أنت مساعد لتقرير سوق أسهم قطري. لكل خبر: عنوان أصلي + ملخص (قد يكون فارغاً). "
        "الغاية: **أقصر نص ممكن** يبقى مفيداً للمستثمر — فقط الوقائع **الجوهريّة** "
        "(قرار، رقم، تاريخ، نسبة، جهة، حدث مباشر على السهم/السوق). احذف: الحشو، "
        "الخلفية، التكرار، التحيات، وما لا يغيّر فهماً للخبر.\n"
        "1) **headline**: سطر واحد بأقل كلمات ممكنة، يوضّح صلب الخبر (من العنوان + الملخص).\n"
        "2) **summary**: جملة أو جملتان قصيرتان كحد أقصى — فقط المعلومة الأهم؛ "
        "لا فقرات. إذا الملخص الأصلي فارغ، اكتب من العنوان فقط دون اختلاق بيانات.\n"
        "أبقِ الأرقام والأسماء والتواريخ والنسب كما في المصدر إذا لزمت المعنى. "
        "لا مقدمات، لا «في إطار»، لا ترقيم نقاط، لا تختلق أرقاماً أو تواريخاً."
    )
    user = (
        f"نوع القسم: {label_ar}.\n"
        "لكل عنصر: JSON بحقلي headline وsummary — أختصر قدر الإمكان؛ الأولوية للمعلومة "
        "الأكثر أهمية فقط.\n"
        f'أعد: {{"items": [{{"headline":"...", "summary":"..."}}, ...]}} '
        f"بنفس الترتيب؛ items.length = {len(rows)} بالضبط.\n\n"
        f"العناصر:\n{json.dumps(payload_items, ensure_ascii=False)}"
    )

    client = OpenAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.2,
            max_tokens=8192,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw = (response.choices[0].message.content or "").strip()
        data = json.loads(raw)
        items = data.get("items")
        if not isinstance(items, list):
            return [("", "")] * len(rows)
        out: list[tuple[str, str]] = []
        for it in items:
            if not isinstance(it, dict):
                out.append(("", ""))
                continue
            h = str(it.get("headline", "")).strip().replace("\n", " ")
            s = str(it.get("summary", "")).strip().replace("\n", " ")
            out.append((h, s))
        while len(out) < len(rows):
            out.append(("", ""))
        return out[: len(rows)]
    except Exception as exc:
        logger.warning("OpenAI news summary failed: %s", exc)
        return [("", "")] * len(rows)

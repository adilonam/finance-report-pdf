from services.page1_service import (
    build_market_chart,
    load_market_last_update,
    load_market_summary,
    load_top_movers,
)


def _to_float_pct(text: str) -> float:
    cleaned = text.replace("%", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _attach_sector_bar_widths(sectors: list) -> list:
    max_abs = max((abs(_to_float_pct(s["change"])) for s in sectors), default=1.0) or 1.0
    for s in sectors:
        ratio = abs(_to_float_pct(s["change"])) / max_abs
        s["bar_width_pct"] = max(4, int(round(ratio * 100)))
        s["is_up"] = "+" in s["change"]
    return sectors


def get_qse_daily_report_data() -> dict:
    sectors = _attach_sector_bar_widths([
        {"sector": "البنوك", "change": "+0.41%"},
        {"sector": "الاتصالات", "change": "+0.88%"},
        {"sector": "الصناعة", "change": "-0.12%"},
        {"sector": "العقار", "change": "-0.33%"},
        {"sector": "الخدمات", "change": "+0.55%"},
        {"sector": "التأمين", "change": "-0.09%"},
        {"sector": "الطاقة", "change": "+0.22%"},
    ])
    market_summary = {
        "index_value": "10,482.3",
        "index_change_points": "+35.4 (+0.34%)",
        "index_change_class": "up",
        "traded_value": "م 437.6 ر.ق",
        "traded_volume": "م 32.1 سهم",
        "commentary": "أغلق المؤشر مرتفعاً %0.34 عند 10,482 بقيمة تداول 437.6 مليون ريال. قاد الاتصالات والخدمات الارتفاع، في حين تراجع العقار بضغط خفيف. المعنويات إيجابية مدعومة بشراء مؤسسي قطري صافي بلغ 18.2 مليون ريال.",
    }
    market_summary.update(load_market_summary())
    report_date = _format_report_date(load_market_last_update()) or "15 April 2026"
    top_movers = load_top_movers()

    return {
        "meta": {
            "report_title_ar": "التقرير اليومي لسوق الأسهم القطري",
            "report_title_en": "Qatar Exchange Daily Market Report",
            "report_date": report_date,
            "session_time": "9:30 ص - 1:15 م",
            "source": "شركة قطر للأوراق المالية",
        },
        "market_summary": market_summary,
        "market_chart": build_market_chart(),
        "top_gainers": top_movers.get("top_gainers") or [
            {"symbol": "ORDS", "price": "13.22", "change": "+1.07%"},
            {"symbol": "IHGS", "price": "15.5", "change": "+0.97%"},
            {"symbol": "QNBK", "price": "17.45", "change": "+0.52%"},
            {"symbol": "CBQK", "price": "6.14", "change": "+0.16%"},
            {"symbol": "NLCS", "price": "2.88", "change": "+0.35%"},
        ],
        "top_losers": top_movers.get("top_losers") or [
            {"symbol": "MARK", "price": "1.93", "change": "-0.51%"},
            {"symbol": "BRES", "price": "4.22", "change": "-0.47%"},
            {"symbol": "WDAM", "price": "3.61", "change": "-0.28%"},
            {"symbol": "QIBK", "price": "19.8", "change": "-0.25%"},
            {"symbol": "QGTS", "price": "2.95", "change": "-0.34%"},
        ],
        "most_active": top_movers.get("most_active") or [
            {"symbol": "QNBK", "price": "17.45", "value": "م89.3"},
            {"symbol": "ORDS", "price": "13.22", "value": "م62.4"},
            {"symbol": "QIBK", "price": "19.80", "value": "م54.7"},
            {"symbol": "CBQK", "price": "6.14", "value": "م38.2"},
            {"symbol": "MARK", "price": "1.930", "value": "م31.8"},
        ],
        "sector_performance": sectors,
        "investor_flow": [
            {
                "title": "Individuals – أفراد",
                "rows": [
                    {"nationality": "عربي", "buy": "م53.3", "sell": "م50.6", "net": "+م2.70", "buy_width": 92, "sell_width": 85},
                    {"nationality": "أجنبي", "buy": "م13.4", "sell": "م10.7", "net": "+م2.68", "buy_width": 24, "sell_width": 17},
                    {"nationality": "خليجي", "buy": "م3.5", "sell": "م1.5", "net": "+م1.97", "buy_width": 6, "sell_width": 1},
                    {"nationality": "قطري", "buy": "م150.8", "sell": "م168.0", "net": "-م17.30", "buy_width": 92, "sell_width": 100},
                ],
            },
            {
                "title": "Institutions – مؤسسات",
                "rows": [
                    {"nationality": "قطري", "buy": "م167.3", "sell": "م149.1", "net": "+م18.20", "buy_width": 100, "sell_width": 89},
                    {"nationality": "خليجي", "buy": "م27.2", "sell": "م28.1", "net": "-م0.99", "buy_width": 16, "sell_width": 17},
                    {"nationality": "أجنبي", "buy": "م133.2", "sell": "م140.5", "net": "-م7.30", "buy_width": 80, "sell_width": 84},
                    {"nationality": "عربي", "buy": "–", "sell": "–", "net": "لا يوجد", "buy_width": 0, "sell_width": 0},
                ],
            },
        ],
        "major_trades": [
            {"symbol": "QNBK", "investor_type": "مؤسسي محلي", "trade_type": "شراء", "type_class": "buy", "quantity": "1,200,000", "avg_price": "17.44", "value": "م20.9", "market_pct": "2.3%"},
            {"symbol": "ORDS", "investor_type": "صندوق أجنبي", "trade_type": "شراء", "type_class": "buy", "quantity": "900,000", "avg_price": "13.20", "value": "م11.9", "market_pct": "1.9%"},
            {"symbol": "QIBK", "investor_type": "مدير تنفيذي", "trade_type": "بيع", "type_class": "sell", "quantity": "250,000", "avg_price": "19.82", "value": "م4.95", "market_pct": "1.1%"},
            {"symbol": "MARK", "investor_type": "مساهم رئيسي", "trade_type": "بيع", "type_class": "sell", "quantity": "1,500,000", "avg_price": "1.930", "value": "م2.9", "market_pct": "1.0%"},
        ],
        "earnings_highlight": {
            "company": "إفصاح بنك قطر الإسلامي QIB – مقارنة الأرباح الفصلية",
            "points": [
                {"label": "Q1 2024", "growth": "", "height": 46},
                {"label": "Q2 2024", "growth": "+1.9%", "height": 58},
                {"label": "Q3 2024", "growth": "+2.2%", "height": 70},
                {"label": "Q4 2024", "growth": "+3.9%", "height": 86},
                {"label": "Q1 2025", "growth": "+4.3%", "height": 106},
                {"label": "Q2 2025", "growth": "+3.2%", "height": 124},
                {"label": "Q3 2025", "growth": "+3.1%", "height": 142},
                {"label": "Q4 2025", "growth": "+3.0%", "height": 166},
                {"label": "Q1 2026", "growth": "+4.9%", "height": 194},
            ],
        },
        "upcoming_events": [
            {"day": "17", "month": "أبريل", "title": "إفصاح أرباح QNBK – بنك قطر الوطني Q1 2026", "subtitle": "النتائج المالية · التوقعات: 4,900 م ر.ق"},
            {"day": "20", "month": "أبريل", "title": "الجمعية العامة لأوريدو ORDS", "subtitle": "التصويت على توزيع الأرباح المقترح: 0.40 ر.ق للسهم"},
            {"day": "22", "month": "أبريل", "title": "آخر يوم للأحقية – MARK مصرف الريان", "subtitle": "توزيع نقدي: 0.05 ر.ق للسهم · صرف: 5 مايو"},
            {"day": "28", "month": "أبريل", "title": "إفصاح أرباح CBQK – Q1 2026", "subtitle": "التوقعات: نمو 8% سنوياً"},
        ],
        "news_highlights": [
            {"label": "إيجابي", "text": "صندوق الثروة السيادي يرفع حصته في القطاع المصرفي بمقدار 1.2 مليار ريال", "tag": "QSE Disclosure · 15 أبريل 2026", "tone": "green"},
            {"label": "إيجابي", "text": "أوريدو تعلن شراكة استراتيجية لتوسيع خدمات الجيل الخامس إقليمياً", "tag": "Ooredoo Press Release · 15 أبريل 2026", "tone": "green"},
            {"label": "سلبي", "text": "تراجع مؤشرات السيولة في العقار مع تثبيت أسعار الفائدة من بنك قطر المركزي", "tag": "QCB Statement · Bloomberg · 14 أبريل 2026", "tone": "red"},
            {"label": "محايد", "text": "QSE تستشير السوق حول تحديث قواعد ملكية المستثمرين الأجانب", "tag": "Qatar Stock Exchange · 13 أبريل 2026", "tone": "gray"},
        ],
        "disclaimer": "شركة قطر للأوراق المالية · Qatar Securities Co. (P.Q.S.C) · للأغراض المعلوماتية فقط · لا تُعد هذه التقرير توصية استثمارية · 15 أبريل 2026",
    }


def _format_report_date(last_update: str) -> str:
    if not last_update:
        return ""

    return last_update.split(" - ", 1)[0].strip()

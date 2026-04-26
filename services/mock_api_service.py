from datetime import date


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
        {"sector": "النقل", "change": "+2.14%"},
        {"sector": "المصارف والخدمات المالية", "change": "+1.12%"},
        {"sector": "الاتصالات", "change": "+0.87%"},
        {"sector": "الصناعة", "change": "+0.65%"},
        {"sector": "السلع الاستهلاكية", "change": "-0.43%"},
        {"sector": "التأمين", "change": "-0.22%"},
        {"sector": "العقار", "change": "-1.87%"},
    ])

    return {
        "meta": {
            "report_title_ar": "التقرير اليومي لبورصة قطر",
            "report_title_en": "Qatar Exchange Daily Market Report",
            "report_date": "16 أبريل 2026",
            "day_name": "الخميس",
            "day_num": "16",
            "month_year": "أبريل 2026",
            "session_time": "9:30 ص - 1:15 م",
            "source": "واجهات تجريبية لبيانات السوق والإفصاحات والأخبار",
        },
        "market_summary": {
            "index_name": "مؤشر بورصة قطر",
            "index_value": "10,714.65",
            "index_change_points": "+92.30 نقطة",
            "index_change_percent": "+0.87%",
            "traded_value": "557.9 م",
            "traded_value_change": "-12.4%",
            "traded_volume": "178.3 م",
            "traded_volume_change": "+3.2%",
            "trades_count": "33,582",
            "trades_count_change": "+5.1%",
        },
        "top_gainers": [
            {"name": "القطرية الألمانية", "value": "12.4 م", "change": "+3.42%"},
            {"name": "بنك الدوحة", "value": "8.1 م", "change": "+3.22%"},
            {"name": "استثمار القابضة", "value": "61.3 م", "change": "+1.85%"},
            {"name": "مصرف الريان", "value": "28.7 م", "change": "+1.42%"},
            {"name": "أوريدو", "value": "47.9 م", "change": "+0.87%"},
        ],
        "top_losers": [
            {"name": "قطر للسينما", "value": "3.2 م", "change": "-9.05%"},
            {"name": "قطر للأسمنت", "value": "5.8 م", "change": "-7.28%"},
            {"name": "المحار القابضة", "value": "2.1 م", "change": "-5.64%"},
            {"name": "فودافون قطر", "value": "4.3 م", "change": "-3.11%"},
            {"name": "قطر للتأمين", "value": "7.6 م", "change": "-1.93%"},
        ],
        "most_active": [
            {"name": "استثمار القابضة", "value": "61.3 م", "volume": "16.8 م"},
            {"name": "أوريدو", "value": "47.9 م", "volume": "5.2 م"},
            {"name": "بنك قطر الوطني", "value": "47.2 م", "volume": "2.1 م"},
            {"name": "بنك قطر التجاري", "value": "32.1 م", "volume": "8.4 م"},
            {"name": "مصرف الريان", "value": "28.7 م", "volume": "16.3 م"},
        ],
        "sector_performance": sectors,
        "shareholder_activity_individuals": [
            {"nationality": "قطري", "sell": "148.8م", "buy": "164.9م", "net": "+16.1م"},
            {"nationality": "عربي", "sell": "46.4م", "buy": "46.4م", "net": "0م"},
            {"nationality": "أجنبي", "sell": "15.9م", "buy": "22.4م", "net": "+6.5م"},
            {"nationality": "خليجي", "sell": "3.1م", "buy": "2.3م", "net": "-0.8م"},
        ],
        "shareholder_activity_institutions": [
            {"nationality": "قطري", "sell": "144.4م", "buy": "150.9م", "net": "+6.5م"},
            {"nationality": "أجنبي", "sell": "153.5م", "buy": "141.4م", "net": "-12.1م"},
            {"nationality": "خليجي", "sell": "45.7م", "buy": "29.6م", "net": "-16.1م"},
            {"nationality": "عربي", "sell": "0.1م", "buy": "0م", "net": "-0.1م"},
        ],
        "major_trades": [
            {"investor": "فيصل علي عبدالله المنصور النعيمي", "buy_qty": "2,126,219", "buy_pct": "1.840%", "sell_qty": "0", "sell_pct": "0.000%"},
            {"investor": "غانم محمد غانم العرابيد الشهواني", "buy_qty": "0", "buy_pct": "0.000%", "sell_qty": "1,167,196", "sell_pct": "1.010%"},
            {"investor": "وساطة للأوراق المالية / صانع السوق", "buy_qty": "1,889,425", "buy_pct": "1.640%", "sell_qty": "1,865,053", "sell_pct": "1.610%"},
            {"investor": "MERRILL LYNCH INTERNATIONAL", "buy_qty": "0", "buy_pct": "0.000%", "sell_qty": "1,663,996", "sell_pct": "1.440%"},
            {"investor": "شركة قطر للأوراق المالية - صانع السوق", "buy_qty": "2,325,913", "buy_pct": "2.010%", "sell_qty": "2,325,913", "sell_pct": "2.010%"},
        ],
        "earnings_highlight": {
            "company": "إفصاح الأرباح الفصلية — بنك قطر الإسلامي QIB",
            "points": [
                {"label": "Q1-25", "value": "620 م", "growth": "+6.9%"},
                {"label": "Q2-25", "value": "658 م", "growth": "+6.1%"},
                {"label": "Q3-25", "value": "701 م", "growth": "+6.5%"},
                {"label": "Q4-25", "value": "743 م", "growth": ""},
                {"label": "Q1-26", "value": "789 م", "growth": ""},
            ],
        },
        "upcoming_events": [
            {"day": "17", "month": "أبريل", "title": "إفصاح أرباح QNBK — بنك قطر الوطني Q1 2026", "subtitle": "التوقعات: 4,900 مليون ر.ق"},
            {"day": "20", "month": "أبريل", "title": "جمعية عامة — ORDS أوريدو", "subtitle": "التصويت على توزيع الأرباح: 0.40 ر.ق للسهم"},
            {"day": "22", "month": "أبريل", "title": "آخر يوم للأحقية — MARK مصرف الريان", "subtitle": "توزيع نقدي: 0.05 ر.ق للسهم · تاريخ الصرف: 5 مايو"},
            {"day": "28", "month": "أبريل", "title": "إفصاح أرباح CBQK — بنك قطر التجاري Q1 2026", "subtitle": "التوقعات: نمو 8% سنوياً"},
        ],
        "news_highlights": [
            {"text": "صندوق الثروة السيادي يرفع حصته في أسهم القطاع المصرفي بمقدار 1.2 مليار ريال قطري.", "tag": "QSE Disclosure · 16 أبريل 2026", "tone": "green"},
            {"text": "أوريدو تعلن شراكة استراتيجية جديدة لتوسيع خدمات الجيل الخامس في المنطقة العربية.", "tag": "Ooredoo Press Release · 16 أبريل 2026", "tone": "green"},
            {"text": "تراجع مؤشرات السيولة في قطاع العقار مع تثبيت أسعار الفائدة من بنك قطر المركزي.", "tag": "QCB Statement · Bloomberg · 15 أبريل 2026", "tone": "red"},
            {"text": "بورصة قطر تطلق مشاورات حول تحديث قواعد تداول الأسهم للمستثمرين الأجانب.", "tag": "Qatar Stock Exchange Official · 14 أبريل 2026", "tone": "blue"},
        ],
        "disclaimer": "Not investment advice · 2026 أبريل 16 · qe.com.qa · المصدر: بورصة قطر · Qatar Securities Company · P.Q.S.C",
        "logo_path": "template/images/logo.png",
    }

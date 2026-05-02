from __future__ import annotations

from datetime import date

from services.api_data_service import (
    build_market_chart,
    load_company_news_highlights,
    load_investor_flow,
    load_major_trades,
    load_market_last_update,
    load_market_summary,
    load_news_highlights,
    load_sector_performance,
    load_top_movers,
    load_upcoming_events,
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


def get_qse_daily_report_data(report_date: date | None = None) -> dict:
    sectors = _attach_sector_bar_widths(load_sector_performance(report_date))
    market_summary = {
        "index_value": "10,482.3",
        "index_change_points": "+35.4 (+0.34%)",
        "index_change_class": "up",
        "traded_value": "م 437.6 ر.ق",
        "traded_volume": "م 32.1 سهم",
        "commentary": "أغلق المؤشر مرتفعاً %0.34 عند 10,482 بقيمة تداول 437.6 مليون ريال. قاد الاتصالات والخدمات الارتفاع، في حين تراجع العقار بضغط خفيف. المعنويات إيجابية مدعومة بشراء مؤسسي قطري صافي بلغ 18.2 مليون ريال.",
    }
    market_summary.update(load_market_summary())
    report_date_label = (
        _format_selected_report_date(report_date)
        or _format_report_date(load_market_last_update())
        or "15 April 2026"
    )
    top_movers = load_top_movers(report_date)
    investor_flow = load_investor_flow(report_date)
    major_trades = load_major_trades(report_date)
    upcoming_events = load_upcoming_events(report_date)
    news_highlights = load_news_highlights() + load_company_news_highlights()

    return {
        "meta": {
            "report_title_ar": "التقرير اليومي لسوق الأسهم القطري",
            "report_title_en": "Qatar Exchange Daily Market Report",
            "report_date": report_date_label,
            "session_time": "9:30 ص - 1:15 م",
            "source": "شركة قطر للأوراق المالية",
        },
        "market_summary": market_summary,
        "market_chart": build_market_chart(),
        "top_gainers": top_movers.get("top_gainers", []),
        "top_losers": top_movers.get("top_losers", []),
        "most_active": top_movers.get("most_active", []),
        "has_top_movers": any(top_movers.values()),
        "sector_performance": sectors,
        "has_sector_performance": bool(sectors),
        "investor_flow": investor_flow,
        "has_investor_flow": bool(investor_flow),
        "major_trades": major_trades,
        "has_major_trades": bool(major_trades),
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
        "upcoming_events": upcoming_events,
        "has_upcoming_events": bool(upcoming_events),
        "news_highlights": news_highlights,
        "has_news_highlights": bool(news_highlights),
        "disclaimer": "شركة قطر للأوراق المالية · Qatar Securities Co. (P.Q.S.C) · للأغراض المعلوماتية فقط · لا تُعد هذه التقرير توصية استثمارية · 15 أبريل 2026",
    }


def _format_report_date(last_update: str) -> str:
    if not last_update:
        return ""

    return last_update.split(" - ", 1)[0].strip()


def _format_selected_report_date(report_date: date | None) -> str:
    if not report_date:
        return ""

    return report_date.strftime("%d %B %Y")

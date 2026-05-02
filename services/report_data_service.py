from __future__ import annotations

from datetime import date

from services.api_data_service import (
    build_market_chart,
    load_company_news_highlights,
    load_earnings_quarters,
    load_investor_flow,
    load_insider_trades,
    load_major_trades,
    load_market_last_update,
    load_market_summary,
    load_news_highlights,
    load_sector_performance,
    load_top_movers,
    load_upcoming_events,
)

_EARNINGS_MAX_BAR_PX = 170
_EARNINGS_AXIS_TICKS = 8


def _to_float_pct(text: str) -> float:
    cleaned = text.replace("%", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _format_earnings_axis_value(value: float) -> str:
    text = f"{value:,.0f}"
    return f"ر.{text}"


def _build_earnings_highlight(rows: list[dict]) -> dict | None:
    if not rows:
        return None

    prices = [float(r["price"]) for r in rows]
    p_min = min(prices)
    p_max = max(prices)
    span = p_max - p_min
    if span <= 0:
        span = abs(p_max) * 0.02 + 1.0
    pad = span * 0.06
    y_low = p_min - pad
    y_high = p_max + pad
    if y_low >= y_high:
        y_low = p_min - 1.0
        y_high = p_max + 1.0

    tick_n = _EARNINGS_AXIS_TICKS
    axis_values = [
        y_high - (y_high - y_low) * i / (tick_n - 1) for i in range(tick_n)
    ]
    y_axis = [_format_earnings_axis_value(v) for v in axis_values]

    points: list[dict] = []
    for index, row in enumerate(rows):
        price = float(row["price"])
        ratio = (price - y_low) / (y_high - y_low) if y_high > y_low else 0.5
        bar_h = max(8.0, min(ratio * _EARNINGS_MAX_BAR_PX, _EARNINGS_MAX_BAR_PX))

        growth = ""
        growth_class = ""
        if index > 0:
            prev = float(rows[index - 1]["price"])
            if prev > 0:
                pct = (price - prev) / prev * 100
                growth = f"{pct:+.1f}%"
                growth_class = "earnings-down" if pct < 0 else ""

        shade_index = index % 9 + 1
        points.append(
            {
                "label": str(row["label"]),
                "growth": growth,
                "growth_class": growth_class,
                "height": round(bar_h, 1),
                "shade_index": shade_index,
            }
        )

    return {"points": points, "y_axis": y_axis}


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
        "traded_value": "437.6 م ر.ق",
        "traded_volume": "32.1 م سهم",
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
    insider_trades = load_insider_trades(report_date)
    upcoming_events = load_upcoming_events(report_date)
    news_highlights = load_news_highlights() + load_company_news_highlights()

    earnings_rows = load_earnings_quarters()
    earnings_highlight = _build_earnings_highlight(earnings_rows)

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
        "insider_trades": insider_trades,
        "has_insider_trades": bool(insider_trades),
        "earnings_highlight": earnings_highlight or {},
        "has_earnings": earnings_highlight is not None,
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

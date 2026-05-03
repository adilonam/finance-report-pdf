from __future__ import annotations

import json
import re
import sqlite3
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path

import requests


QE20_TIME_LEVEL_URL = "https://www.qe.com.qa/wp/mw/data/QE20_TimeLevel.txt"
MARKET_INDEX_URL = "https://www.qe.com.qa/wp/mw/data/Index.txt"
TOP5_URL_TEMPLATE = "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/Top5.txt"
INDICES_SUMMARY_URL_TEMPLATE = "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/IndicesSummary.txt"
INVESTOR_ACTIVITY_URL_TEMPLATE = "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/InvestorActivity.txt"
MAJOR_ACTIVITY_URL_TEMPLATE = "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/MajorActivity.txt"
INSIDER_TRADES_URL_TEMPLATE = (
    "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/InsiderTrades.txt"
)
EVENTS_URL = "https://www.qe.com.qa/wp/mw_app/mw.php"
MORE_INFORMATION_SEARCH_URL = "https://www.qe.com.qa/ar/moreinformationsearch"
COMPANY_MORE_INFORMATION_SEARCH_URL = (
    "https://www.qe.com.qa/ar/companymoreinformationsearch"
)
OUTPUT_XML_JS_PATTERN = re.compile(
    r"var\s+outputXML\s*=\s*'([^']*)'",
    re.IGNORECASE | re.DOTALL,
)
OUTPUT_XML_JS_PATTERN_DQ = re.compile(
    r'var\s+outputXML\s*=\s*"([^"]*)"',
    re.IGNORECASE | re.DOTALL,
)
NEWS_HIGHLIGHTS_LIMIT = 4
DATABASE_DIR = Path("database")
DATABASE_PATH = DATABASE_DIR / "finance_report.sqlite3"
TOP5_JSON_PREFIX = "for(;;);"
QE20_TOPIC = "QE20/ID"
MARKET_INDEX_TABLE = "market_index"
QE20_TIME_LEVEL_TABLE = "qe20_time_level"
TOP_MOVERS_TABLE = "top_movers"
SECTOR_PERFORMANCE_TABLE = "sector_performance"
INVESTOR_ACTIVITY_TABLE = "investor_activity"
MAJOR_ACTIVITY_TABLE = "major_activity"
INSIDER_TRADES_TABLE = "insider_trades"
EARNINGS_QUARTERS_TABLE = "earnings_quarters"
UPCOMING_EVENTS_TABLE = "upcoming_events"
EXCHANGE_NEWS_TABLE = "exchange_news"
COMPANY_NEWS_TABLE = "company_news"
LISTED_COMPANIES_TABLE = "listed_companies"
ALL_TABLES = [
    MARKET_INDEX_TABLE,
    QE20_TIME_LEVEL_TABLE,
    TOP_MOVERS_TABLE,
    SECTOR_PERFORMANCE_TABLE,
    INVESTOR_ACTIVITY_TABLE,
    MAJOR_ACTIVITY_TABLE,
    INSIDER_TRADES_TABLE,
    UPCOMING_EVENTS_TABLE,
    EXCHANGE_NEWS_TABLE,
    COMPANY_NEWS_TABLE,
]
SECTOR_INDEX_NAMES = {
    "QBNK": "البنوك والخدمات المالية",
    "QCON": "الخدمات والسلع الاستهلاكية",
    "QIND": "الصناعة",
    "QINS": "التأمين",
    "QREA": "العقارات",
    "QTLC": "الاتصالات",
    "QTRN": "النقل",
}
INVESTOR_TYPE_LABELS = {
    "I": "Individuals – أفراد",
    "C": "Institutions – مؤسسات",
}
INVESTOR_TYPE_ORDER = ["I", "C"]
NATIONALITY_LABELS = {
    "ARB": "عربي",
    "FRN": "أجنبي",
    "GCC": "خليجي",
    "QTR": "قطري",
}
NATIONALITY_ORDER_BY_INVESTOR_TYPE = {
    "I": ["ARB", "FRN", "GCC", "QTR"],
    "C": ["QTR", "GCC", "FRN", "ARB"],
}

FALLBACK_MARKET_CHART = {
    "labels": ["9:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "1:00"],
    "y_labels": ["10,500", "10,490", "10,480", "10,470", "10,460", "10,450", "10,440"],
    "line_path": "M0 132 C55 122 81 126 109 125 C157 124 195 100 257 95 C311 88 341 104 389 96 C443 87 470 67 508 72 C557 78 573 68 617 65 C670 62 689 79 734 69 C787 55 815 44 870 46 C918 49 942 42 1000 40",
    "area_path": "M0 132 C55 122 81 126 109 125 C157 124 195 100 257 95 C311 88 341 104 389 96 C443 87 470 67 508 72 C557 78 573 68 617 65 C670 62 689 79 734 69 C787 55 815 44 870 46 C918 49 942 42 1000 40 L1000 160 L0 160 Z",
}


def fetch_qe20_time_level() -> list[dict]:
    response = requests.get(QE20_TIME_LEVEL_URL, timeout=20)
    if response.status_code == 404:
        return []

    response.raise_for_status()
    return _as_dict_rows(response.json())


def fetch_market_index() -> list[dict]:
    response = requests.get(MARKET_INDEX_URL, timeout=20)
    if response.status_code == 404:
        return []

    response.raise_for_status()
    payload = response.json()
    return _rows_from_payload(payload)


def fetch_top_movers(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    url = _dated_feed_url(TOP5_URL_TEMPLATE, current_date)
    response = requests.get(url, timeout=20)
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    return current_date.isoformat(), _parse_top5_response(response.content)


def fetch_sector_performance(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    url = _dated_feed_url(INDICES_SUMMARY_URL_TEMPLATE, current_date)
    response = requests.get(url, timeout=20)
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    return current_date.isoformat(), _parse_top5_response(response.content)


def fetch_investor_activity(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    url = _dated_feed_url(INVESTOR_ACTIVITY_URL_TEMPLATE, current_date)
    response = requests.get(url, timeout=20)
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    return current_date.isoformat(), _parse_top5_response(response.content)


def fetch_major_activity(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    url = _dated_feed_url(MAJOR_ACTIVITY_URL_TEMPLATE, current_date)
    response = requests.get(url, timeout=20)
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    return current_date.isoformat(), _parse_top5_response(response.content)


def fetch_insider_trades(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    url = _dated_feed_url(INSIDER_TRADES_URL_TEMPLATE, current_date)
    response = requests.get(url, timeout=20)
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    return current_date.isoformat(), _parse_top5_response(response.content)


def fetch_upcoming_events(
    as_of_date: date | None = None,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()
    response = requests.post(
        EVENTS_URL,
        data={
            "f": "Events",
            "p": "-1",
            "l": "ar",
            "s": current_date.isoformat(),
            "e": current_date.isoformat(),
        },
        timeout=20,
    )
    if response.status_code == 404:
        return current_date.isoformat(), []

    response.raise_for_status()
    payload = response.json()
    return current_date.isoformat(), _rows_from_payload(payload)


def _qse_news_page_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    }


def _fetch_qse_news_from_page(url: str) -> list[dict]:
    try:
        response = requests.get(url, timeout=30, headers=_qse_news_page_headers())
    except requests.RequestException:
        return []

    if response.status_code != 200:
        return []

    return _parse_qse_news_output_xml_page(response.text)


def fetch_exchange_news() -> list[dict]:
    return _fetch_qse_news_from_page(MORE_INFORMATION_SEARCH_URL)


def fetch_company_news() -> list[dict]:
    return _fetch_qse_news_from_page(COMPANY_MORE_INFORMATION_SEARCH_URL)


def _parse_qse_news_output_xml_page(html: str) -> list[dict]:
    raw = _extract_output_xml_js_value(html)
    if not raw:
        return []

    xml_text = urllib.parse.unquote_plus(raw)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    err = root.find("ServletErrorStatus")
    if err is not None and (err.text or "").strip() != "0":
        return []

    return _news_rows_from_output_xml(root)


def _extract_output_xml_js_value(html: str) -> str:
    match = OUTPUT_XML_JS_PATTERN.search(html)
    if match:
        return match.group(1)

    match = OUTPUT_XML_JS_PATTERN_DQ.search(html)
    if match:
        return match.group(1)

    return ""


def _news_rows_from_output_xml(root: ET.Element) -> list[dict]:
    rows: list[dict] = []
    for news_el in root.findall(".//News"):
        detail_id = (news_el.findtext("InformationTypeDetailID") or "").strip()
        headline = (news_el.findtext("Headline") or "").strip()
        if not detail_id or not headline:
            continue

        rows.append(
            {
                "detail_id": detail_id,
                "headline": headline,
                "summary": (news_el.findtext("Summary") or "").strip(),
                "publish_date": (news_el.findtext("PublishDate") or "").strip(),
            }
        )

    return rows


def save_exchange_news(rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["detail_id"]),
            str(row["headline"]),
            str(row.get("summary", "")),
            str(row.get("publish_date", "")),
            index,
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and row.get("detail_id") and row.get("headline")
    ]

    with _connect() as connection:
        connection.execute(f"DELETE FROM {EXCHANGE_NEWS_TABLE}")
        connection.executemany(
            f"""
            INSERT INTO {EXCHANGE_NEWS_TABLE} (
                detail_id,
                headline,
                summary,
                publish_date,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_company_news(rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["detail_id"]),
            str(row["headline"]),
            str(row.get("summary", "")),
            str(row.get("publish_date", "")),
            index,
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and row.get("detail_id") and row.get("headline")
    ]

    with _connect() as connection:
        connection.execute(f"DELETE FROM {COMPANY_NEWS_TABLE}")
        connection.executemany(
            f"""
            INSERT INTO {COMPANY_NEWS_TABLE} (
                detail_id,
                headline,
                summary,
                publish_date,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def load_news_highlights(limit: int = NEWS_HIGHLIGHTS_LIMIT) -> list[dict]:
    return _load_news_rows_for_template(
        EXCHANGE_NEWS_TABLE,
        "أخبار البورصة",
        "QSE",
        limit,
    )


def load_company_news_highlights(limit: int = NEWS_HIGHLIGHTS_LIMIT) -> list[dict]:
    return _load_news_rows_for_template(
        COMPANY_NEWS_TABLE,
        "أخبار الشركات",
        "شركات",
        limit,
    )


def _load_news_rows_for_template(
    table_name: str,
    label_ar: str,
    tag_brand: str,
    limit: int,
) -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT headline, summary, publish_date
            FROM {table_name}
            ORDER BY sort_order ASC
            LIMIT ?
            """,
            (limit,),
        )
        db_rows = cursor.fetchall()

    return [
        {
            "label": label_ar,
            "text": headline,
            "tag": _format_news_highlight_tag(summary, publish_date, tag_brand),
            "tone": "gray",
        }
        for headline, summary, publish_date in db_rows
    ]


def _format_news_highlight_tag(
    summary: str,
    publish_date_iso: str,
    brand: str,
) -> str:
    date_part = _format_arabic_datetime_line(publish_date_iso)
    if summary and len(summary) <= 120:
        return f"{summary} · {date_part}" if date_part else summary
    if date_part:
        return f"{brand} · {date_part}"
    return brand


def _format_arabic_datetime_line(iso_value: str) -> str:
    if not iso_value:
        return ""

    cleaned = iso_value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return iso_value

    month_ar = _arabic_month_name(dt.month)
    return f"{dt.day} {month_ar} {dt.year}"


def _arabic_month_name(month: int) -> str:
    names = {
        1: "يناير",
        2: "فبراير",
        3: "مارس",
        4: "أبريل",
        5: "مايو",
        6: "يونيو",
        7: "يوليو",
        8: "أغسطس",
        9: "سبتمبر",
        10: "أكتوبر",
        11: "نوفمبر",
        12: "ديسمبر",
    }
    return names.get(month, str(month))


def _dated_feed_url(url_template: str, as_of_date: date) -> str:
    return url_template.format(
        year=as_of_date.strftime("%Y"),
        month=as_of_date.strftime("%m"),
        day=as_of_date.strftime("%d"),
    )


def _parse_top5_response(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig").strip()
    if text.startswith(TOP5_JSON_PREFIX):
        text = text[len(TOP5_JSON_PREFIX):]

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    return _rows_from_payload(payload)


def _rows_from_payload(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        return _as_dict_rows(payload.get("rows", []))

    return _as_dict_rows(payload)


def _as_dict_rows(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []

    return [row for row in value if isinstance(row, dict)]


def _connect() -> sqlite3.Connection:
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS qe20_time_level (
            time TEXT PRIMARY KEY,
            index_value REAL NOT NULL,
            volume INTEGER NOT NULL,
            sort_order INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS top_movers (
            data_type TEXT NOT NULL,
            symbol_code TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            traded_value REAL NOT NULL,
            close_price REAL NOT NULL,
            report_date TEXT NOT NULL,
            date_type TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            PRIMARY KEY (data_type, symbol_code)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_index (
            topic TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            sort_order INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_performance (
            index_code TEXT PRIMARY KEY,
            index_name TEXT NOT NULL,
            closing_value REAL NOT NULL,
            change_value REAL NOT NULL,
            change_pct REAL NOT NULL,
            report_date TEXT NOT NULL,
            date_type TEXT NOT NULL,
            priority INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS investor_activity (
            natgrp TEXT NOT NULL,
            invtype TEXT NOT NULL,
            trade_type TEXT NOT NULL,
            companies INTEGER NOT NULL,
            traded_value REAL NOT NULL,
            traded_volume INTEGER NOT NULL,
            trade_pct REAL NOT NULL,
            report_date TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            PRIMARY KEY (natgrp, invtype, trade_type)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS major_activity (
            symbol_code TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            investor_name TEXT NOT NULL,
            trade_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            market_pct REAL NOT NULL,
            report_date TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            PRIMARY KEY (symbol_code, investor_name, trade_type)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS insider_trades (
            report_date TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            symbol_code TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            insider_name TEXT NOT NULL,
            trade_side TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (report_date, sort_order)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS earnings_quarters (
            sort_order INTEGER NOT NULL,
            label TEXT NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY (sort_order)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS listed_companies (
            symbol TEXT PRIMARY KEY NOT NULL,
            company_name TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS upcoming_events (
            event_date TEXT NOT NULL,
            news_id TEXT NOT NULL,
            news_time TEXT NOT NULL,
            news_title TEXT NOT NULL,
            news_theme TEXT NOT NULL,
            comp_code TEXT NOT NULL,
            news_comp TEXT NOT NULL,
            display_flag TEXT NOT NULL,
            sort_order INTEGER NOT NULL,
            PRIMARY KEY (event_date, news_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_news (
            detail_id TEXT NOT NULL PRIMARY KEY,
            headline TEXT NOT NULL,
            summary TEXT NOT NULL,
            publish_date TEXT NOT NULL,
            sort_order INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS company_news (
            detail_id TEXT NOT NULL PRIMARY KEY,
            headline TEXT NOT NULL,
            summary TEXT NOT NULL,
            publish_date TEXT NOT NULL,
            sort_order INTEGER NOT NULL
        )
        """
    )
    return connection


def save_market_index(rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["Topic"]),
            json.dumps(row, ensure_ascii=False),
            int(row.get("Order", index)),
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and row.get("Topic")
    ]

    with _connect() as connection:
        connection.execute("DELETE FROM market_index")
        connection.executemany(
            """
            INSERT INTO market_index (topic, payload, sort_order)
            VALUES (?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_qe20_time_level(rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["Time"]),
            float(row["Index"]),
            int(row.get("Volume", 0)),
            int(row.get("SortOrder", index)),
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and "Time" in row and "Index" in row
    ]

    with _connect() as connection:
        connection.execute("DELETE FROM qe20_time_level")
        connection.executemany(
            """
            INSERT INTO qe20_time_level (time, index_value, volume, sort_order)
            VALUES (?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_top_movers(report_date: str, rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["DATA_TYPE"]),
            str(row["SYMBOL_CODE"]),
            str(row.get("SYMBOL_NAME_3") or row.get("SYMBOL_NAME_4") or row["SYMBOL_CODE"]),
            float(row.get("TVALUE", 0)),
            float(row.get("CLOSE_PRICE", 0)),
            report_date,
            str(row.get("DATE_TYPE", "Day")),
            index,
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and "DATA_TYPE" in row and "SYMBOL_CODE" in row
    ]

    with _connect() as connection:
        connection.execute("DELETE FROM top_movers")
        connection.executemany(
            """
            INSERT INTO top_movers (
                data_type,
                symbol_code,
                symbol_name,
                traded_value,
                close_price,
                report_date,
                date_type,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_sector_performance(report_date: str, rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["INDEX_CODE"]),
            SECTOR_INDEX_NAMES[str(row["INDEX_CODE"])],
            float(row.get("INDEX_CLOSING_VALUE", 0)),
            float(row.get("CHANGE_VALUE", 0)),
            float(row.get("CHANGE_PRCT", 0)),
            report_date,
            str(row.get("DATE_TYPE", "Day")),
            int(row.get("PRIORITY", index)),
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict) and row.get("INDEX_CODE") in SECTOR_INDEX_NAMES
    ]

    with _connect() as connection:
        connection.execute("DELETE FROM sector_performance")
        connection.executemany(
            """
            INSERT INTO sector_performance (
                index_code,
                index_name,
                closing_value,
                change_value,
                change_pct,
                report_date,
                date_type,
                priority
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_investor_activity(report_date: str, rows: list[dict]) -> int:
    clean_rows = []
    sort_order = 0
    for nationality in rows:
        if not isinstance(nationality, dict):
            continue

        natgrp = str(nationality.get("natgrp", ""))
        if natgrp not in NATIONALITY_LABELS:
            continue

        for investor_type in nationality.get("Data", []):
            invtype = str(investor_type.get("invtype", ""))
            if invtype not in INVESTOR_TYPE_LABELS:
                continue

            for trade in investor_type.get("Data", []):
                trade_type = str(trade.get("trade_type", ""))
                if trade_type not in {"Buy", "Sell"}:
                    continue

                clean_rows.append(
                    (
                        natgrp,
                        invtype,
                        trade_type,
                        int(trade.get("companies", 0)),
                        float(trade.get("traded_value", 0)),
                        int(trade.get("traded_volume", 0)),
                        float(trade.get("prct", 0)),
                        report_date,
                        sort_order,
                    )
                )
                sort_order += 1

    with _connect() as connection:
        connection.execute("DELETE FROM investor_activity")
        connection.executemany(
            """
            INSERT INTO investor_activity (
                natgrp,
                invtype,
                trade_type,
                companies,
                traded_value,
                traded_volume,
                trade_pct,
                report_date,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_major_activity(report_date: str, rows: list[dict]) -> int:
    clean_rows = []
    sort_order = 0
    for symbol in rows:
        if not isinstance(symbol, dict):
            continue

        symbol_code = str(symbol.get("symbol_code", ""))
        symbol_name = str(
            symbol.get("symbol_name_3")
            or symbol.get("symbol_name_4")
            or symbol_code
        )
        if not symbol_code:
            continue

        for investor in symbol.get("Data", []):
            investor_name = str(investor.get("nin_name", ""))
            if not investor_name:
                continue

            for trade_type, volume_key, pct_key in [
                ("buy", "buy_volume", "net_buy"),
                ("sell", "sell_volume", "net_sell"),
            ]:
                quantity = int(investor.get(volume_key, 0))
                market_pct = float(investor.get(pct_key, 0))
                if quantity <= 0 or market_pct <= 0:
                    continue

                clean_rows.append(
                    (
                        symbol_code,
                        symbol_name,
                        investor_name,
                        trade_type,
                        quantity,
                        market_pct,
                        report_date,
                        sort_order,
                    )
                )
                sort_order += 1

    with _connect() as connection:
        connection.execute("DELETE FROM major_activity")
        connection.executemany(
            """
            INSERT INTO major_activity (
                symbol_code,
                symbol_name,
                investor_name,
                trade_type,
                quantity,
                market_pct,
                report_date,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_insider_trades(report_date: str, rows: list[dict]) -> int:
    clean_rows: list[tuple[str, int, str, str, str, str, int]] = []
    sort_order = 0
    for row in rows:
        if not isinstance(row, dict):
            continue

        symbol_code = str(row.get("SYMBOL_CODE", "")).strip()
        if not symbol_code:
            continue

        symbol_name = str(
            row.get("SYMBOL_NAME")
            or row.get("SYMBOL_NAME_2")
            or row.get("SYMBOL_NAME_3")
            or row.get("SYMBOL_NAME_4")
            or symbol_code
        ).strip()
        insider_name = str(row.get("NIN_NAME", "")).strip()
        if not insider_name:
            continue

        buy = row.get("BUY")
        sell = row.get("SELL")
        if buy is not None:
            clean_rows.append(
                (
                    report_date,
                    sort_order,
                    symbol_code,
                    symbol_name,
                    insider_name,
                    "buy",
                    int(buy),
                )
            )
            sort_order += 1
        if sell is not None:
            clean_rows.append(
                (
                    report_date,
                    sort_order,
                    symbol_code,
                    symbol_name,
                    insider_name,
                    "sell",
                    int(sell),
                )
            )
            sort_order += 1

    with _connect() as connection:
        connection.execute(f"DELETE FROM {INSIDER_TRADES_TABLE}")
        connection.executemany(
            f"""
            INSERT INTO {INSIDER_TRADES_TABLE} (
                report_date,
                sort_order,
                symbol_code,
                symbol_name,
                insider_name,
                trade_side,
                quantity
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def save_upcoming_events(report_date: str, rows: list[dict]) -> int:
    clean_rows = []
    sort_order = 0
    for day_group in rows:
        if not isinstance(day_group, dict):
            continue

        event_date = str(day_group.get("date") or report_date)
        for event in day_group.get("data", []):
            news_id = str(event.get("newsID", ""))
            news_title = str(event.get("newsTitle", "")).strip()
            if not news_id or not news_title:
                continue

            clean_rows.append(
                (
                    event_date,
                    news_id,
                    str(event.get("newsTime", "")).strip(),
                    news_title,
                    str(event.get("newsTheme", "")).strip(),
                    str(event.get("compCode", "")).strip(),
                    str(event.get("newsComp", "")).strip(),
                    str(event.get("newsDisplay", "")).strip(),
                    sort_order,
                )
            )
            sort_order += 1

    with _connect() as connection:
        connection.execute("DELETE FROM upcoming_events WHERE event_date = ?", (report_date,))
        connection.executemany(
            """
            INSERT INTO upcoming_events (
                event_date,
                news_id,
                news_time,
                news_title,
                news_theme,
                comp_code,
                news_comp,
                display_flag,
                sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            clean_rows,
        )
    return len(clean_rows)


def run_update_jobs(as_of_date: date | None = None) -> dict:
    clear_all_tables()
    market_index_rows = fetch_market_index()
    market_index_count = save_market_index(market_index_rows)
    market_index_date = _get_market_index_date(market_index_rows)
    market_index_last_update = _get_market_index_last_update(market_index_rows)
    report_date = as_of_date or _parse_report_date(market_index_date) or date.today()

    chart_rows = fetch_qe20_time_level()
    chart_date_from, chart_date_to = _get_chart_date_range(chart_rows)
    chart_data_date = _get_chart_data_date(chart_rows)
    chart_count = save_qe20_time_level(chart_rows)

    top_movers_date, top_mover_rows = fetch_top_movers(report_date)
    top_movers_count = save_top_movers(top_movers_date, top_mover_rows)

    sector_performance_date, sector_performance_rows = fetch_sector_performance(report_date)
    sector_performance_count = save_sector_performance(
        sector_performance_date,
        sector_performance_rows,
    )

    investor_activity_date, investor_activity_rows = fetch_investor_activity(report_date)
    investor_activity_count = save_investor_activity(
        investor_activity_date,
        investor_activity_rows,
    )

    major_activity_date, major_activity_rows = fetch_major_activity(report_date)
    major_activity_count = save_major_activity(
        major_activity_date,
        major_activity_rows,
    )

    insider_trades_date, insider_trades_rows = fetch_insider_trades(report_date)
    insider_trades_count = save_insider_trades(
        insider_trades_date,
        insider_trades_rows,
    )

    events_date, event_rows = fetch_upcoming_events(report_date)
    events_count = save_upcoming_events(events_date, event_rows)

    exchange_news_rows = fetch_exchange_news()
    exchange_news_count = save_exchange_news(exchange_news_rows)

    company_news_rows = fetch_company_news()
    company_news_count = save_company_news(company_news_rows)

    result = {
        "requested_report_date": report_date.isoformat(),
        "market_index_rows": market_index_count,
        "market_index_date": market_index_date,
        "market_index_last_update": market_index_last_update,
        "market_index_table": MARKET_INDEX_TABLE,
        "chart_rows": chart_count,
        "chart_data_date": chart_data_date,
        "chart_date_from": chart_date_from,
        "chart_date_to": chart_date_to,
        "chart_table": QE20_TIME_LEVEL_TABLE,
        "top_movers_rows": top_movers_count,
        "top_movers_date": top_movers_date,
        "top_movers_table": TOP_MOVERS_TABLE,
        "sector_performance_rows": sector_performance_count,
        "sector_performance_date": sector_performance_date,
        "sector_performance_table": SECTOR_PERFORMANCE_TABLE,
        "investor_activity_rows": investor_activity_count,
        "investor_activity_date": investor_activity_date,
        "investor_activity_table": INVESTOR_ACTIVITY_TABLE,
        "major_activity_rows": major_activity_count,
        "major_activity_date": major_activity_date,
        "major_activity_table": MAJOR_ACTIVITY_TABLE,
        "insider_trades_rows": insider_trades_count,
        "insider_trades_date": insider_trades_date,
        "insider_trades_table": INSIDER_TRADES_TABLE,
        "upcoming_events_rows": events_count,
        "upcoming_events_date": events_date,
        "upcoming_events_table": UPCOMING_EVENTS_TABLE,
        "exchange_news_rows": exchange_news_count,
        "exchange_news_table": EXCHANGE_NEWS_TABLE,
        "company_news_rows": company_news_count,
        "company_news_table": COMPANY_NEWS_TABLE,
        "database_path": str(DATABASE_PATH),
    }
    result["log_lines"] = _build_run_job_log_lines(result)
    return result


def clear_all_tables() -> None:
    with _connect() as connection:
        for table_name in ALL_TABLES:
            connection.execute(f"DELETE FROM {table_name}")


def _build_run_job_log_lines(result: dict) -> list[str]:
    return [
        "Run Job completed successfully.",
        f"Database: {result['database_path']}",
        f"Requested report date: {result['requested_report_date']}",
        "APIs processed: 10",
        "",
        "[1] Market Index API - Index.txt",
        f"Data date: {result['market_index_date'] or 'N/A'}",
        f"Last update: {result['market_index_last_update'] or 'N/A'}",
        f"Rows added: {result['market_index_rows']}",
        f"Table affected: {result['market_index_table']}",
        "",
        "[2] QE20 Intraday Chart API - QE20_TimeLevel.txt",
        f"Data date: {result['chart_data_date'] or 'N/A'}",
        f"Time range: {result['chart_date_from'] or 'N/A'} -> {result['chart_date_to'] or 'N/A'}",
        f"Rows added: {result['chart_rows']}",
        f"Table affected: {result['chart_table']}",
        "",
        "[3] Top Movers API - Top5.txt",
        f"Data date: {result['top_movers_date'] or 'N/A'}",
        f"Rows added: {result['top_movers_rows']}",
        f"Table affected: {result['top_movers_table']}",
        "",
        "[4] Sector Performance API - IndicesSummary.txt",
        f"Data date: {result['sector_performance_date'] or 'N/A'}",
        f"Rows added: {result['sector_performance_rows']}",
        f"Table affected: {result['sector_performance_table']}",
        "",
        "[5] Investor Flow API - InvestorActivity.txt",
        f"Data date: {result['investor_activity_date'] or 'N/A'}",
        f"Rows added: {result['investor_activity_rows']}",
        f"Table affected: {result['investor_activity_table']}",
        "",
        "[6] Major Trades API - MajorActivity.txt",
        f"Data date: {result['major_activity_date'] or 'N/A'}",
        f"Rows added: {result['major_activity_rows']}",
        f"Table affected: {result['major_activity_table']}",
        "",
        "[7] Insider Trades API - InsiderTrades.txt",
        f"Data date: {result['insider_trades_date'] or 'N/A'}",
        f"Rows added: {result['insider_trades_rows']}",
        f"Table affected: {result['insider_trades_table']}",
        "",
        "[8] Upcoming Events API - mw.php f=Events",
        f"Data date: {result['upcoming_events_date'] or 'N/A'}",
        f"Rows added: {result['upcoming_events_rows']}",
        f"Table affected: {result['upcoming_events_table']}",
        "",
        "[9] Exchange News - moreinformationsearch (outputXML)",
        f"Rows added: {result['exchange_news_rows']}",
        f"Table affected: {result['exchange_news_table']}",
        "",
        "[10] Company News - companymoreinformationsearch (outputXML)",
        f"Rows added: {result['company_news_rows']}",
        f"Table affected: {result['company_news_table']}",
    ]


def _get_market_index_date(rows: list[dict]) -> str:
    row = _get_preferred_market_index_row(rows)
    return str(row.get("Date", ""))


def _get_market_index_last_update(rows: list[dict]) -> str:
    row = _get_preferred_market_index_row(rows)
    return str(row.get("LastUpdate", ""))


def _parse_report_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        return None


def _get_preferred_market_index_row(rows: list[dict]) -> dict:
    for row in rows:
        if row.get("Topic") == QE20_TOPIC:
            return row
    return rows[0] if rows else {}


def _get_chart_date_range(rows: list[dict]) -> tuple[str, str]:
    values = [
        str(row["Time"])
        for row in rows
        if row.get("Time")
    ]
    if not values:
        return "", ""

    return values[0], values[-1]


def _get_chart_data_date(rows: list[dict]) -> str:
    first_time = next((str(row["Time"]) for row in rows if row.get("Time")), "")
    return first_time.split(" ", 1)[0]


def load_qe20_time_level() -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    with _connect() as connection:
        cursor = connection.execute(
            """
            SELECT time, index_value
            FROM qe20_time_level
            ORDER BY sort_order ASC
            """
        )
        return [{"time": row[0], "index": row[1]} for row in cursor.fetchall()]


def load_market_summary() -> dict:
    market_index = load_market_index_row()
    if not market_index:
        return {}

    return _build_market_summary(market_index)


def load_market_last_update() -> str:
    market_index = load_market_index_row()
    return str(market_index.get("LastUpdate", "")) if market_index else ""


def load_market_index_row() -> dict:
    if not DATABASE_PATH.exists():
        return {}

    with _connect() as connection:
        cursor = connection.execute(
            """
            SELECT payload
            FROM market_index
            ORDER BY CASE WHEN topic = ? THEN 0 ELSE 1 END, sort_order ASC
            LIMIT 1
            """,
            (QE20_TOPIC,),
        )
        row = cursor.fetchone()

    if not row:
        return {}

    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return {}


def _build_market_summary(market_index: dict) -> dict:
    last_price = _to_float(market_index.get("LastPrice"))
    change = _to_float(market_index.get("Change"))
    percent_change = _to_float(market_index.get("PercentChange"))
    traded_value = _to_float(market_index.get("Value"))
    traded_volume = _to_float(market_index.get("Volume"))

    direction_class = "up" if change > 0 else "down" if change < 0 else ""
    direction_text = "مرتفعاً" if change > 0 else "منخفضاً" if change < 0 else "دون تغيير"

    return {
        "index_value": f"{last_price:,.2f}",
        "index_change_points": f"{_format_signed_decimal(change)} ({_format_signed_fixed(percent_change, 2)}%)",
        "index_change_class": direction_class,
        "traded_value": f"{traded_value / 1_000_000:.1f} م ر.ق",
        "traded_volume": f"{traded_volume / 1_000_000:.1f} م سهم",
        "commentary": (
            f"أغلق مؤشر QE {direction_text} عند {last_price:,.2f} "
            f"بتغيير {_format_signed_decimal(change)} نقطة "
            f"({_format_signed_fixed(percent_change, 2)}%)، وبلغت قيمة التداول "
            f"{traded_value:,.0f} ر.ق على حجم {traded_volume:,.0f} سهم."
        ),
    }


def load_top_movers(as_of_date: date | None = None) -> dict:
    if not DATABASE_PATH.exists():
        return {}

    where_clause = "WHERE report_date = ?" if as_of_date else ""
    parameters = (as_of_date.isoformat(),) if as_of_date else ()
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT data_type, symbol_code, traded_value, close_price
            FROM top_movers
            {where_clause}
            ORDER BY sort_order ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    if not rows:
        return {}

    return {
        "top_gainers": [
            {
                "symbol": symbol,
                "price": _format_decimal(close_price),
                "change": _format_pct(value, signed=True),
            }
            for data_type, symbol, value, close_price in rows
            if data_type == "TOP5GAINER"
        ],
        "top_losers": [
            {
                "symbol": symbol,
                "price": _format_decimal(close_price),
                "change": _format_pct(value),
            }
            for data_type, symbol, value, close_price in rows
            if data_type == "TOP5LOSER"
        ],
        "most_active": [
            {
                "symbol": symbol,
                "price": _format_decimal(close_price),
                "value": _format_millions(value),
            }
            for data_type, symbol, value, close_price in rows
            if data_type == "TOP5VALUE"
        ],
    }


def load_sector_performance(as_of_date: date | None = None) -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    where_clause = "WHERE report_date = ?" if as_of_date else ""
    parameters = (as_of_date.isoformat(),) if as_of_date else ()
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT index_name, change_pct
            FROM sector_performance
            {where_clause}
            ORDER BY priority ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    return [
        {
            "sector": index_name,
            "change": _format_pct(change_pct, signed=True),
        }
        for index_name, change_pct in rows
    ]


def load_investor_flow(as_of_date: date | None = None) -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    where_clause = "WHERE report_date = ?" if as_of_date else ""
    parameters = (as_of_date.isoformat(),) if as_of_date else ()
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT natgrp, invtype, trade_type, traded_value
            FROM investor_activity
            {where_clause}
            ORDER BY sort_order ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    if not rows:
        return []

    values_by_key: dict[tuple[str, str], dict[str, float]] = {}
    for natgrp, invtype, trade_type, traded_value in rows:
        key = (natgrp, invtype)
        values_by_key.setdefault(key, {"Buy": 0.0, "Sell": 0.0})
        values_by_key[key][trade_type] = traded_value

    max_value = max(
        (value for totals in values_by_key.values() for value in totals.values()),
        default=1.0,
    ) or 1.0

    groups = []
    for invtype in INVESTOR_TYPE_ORDER:
        group_rows = []
        for natgrp in NATIONALITY_ORDER_BY_INVESTOR_TYPE[invtype]:
            if (natgrp, invtype) not in values_by_key:
                group_rows.append(
                    {
                        "nationality": NATIONALITY_LABELS[natgrp],
                        "buy": "–",
                        "sell": "–",
                        "net": "لا يوجد",
                        "buy_width": 0,
                        "sell_width": 0,
                    }
                )
                continue

            totals = values_by_key[(natgrp, invtype)]
            buy_value = totals["Buy"]
            sell_value = totals["Sell"]
            group_rows.append(
                {
                    "nationality": NATIONALITY_LABELS[natgrp],
                    "buy": _format_millions(buy_value),
                    "sell": _format_millions(sell_value),
                    "net": _format_net_millions(buy_value - sell_value),
                    "buy_width": _bar_width(buy_value, max_value),
                    "sell_width": _bar_width(sell_value, max_value),
                }
            )

        if group_rows:
            groups.append(
                {
                    "title": INVESTOR_TYPE_LABELS[invtype],
                    "rows": group_rows,
                }
            )

    return groups


def load_major_trades(as_of_date: date | None = None) -> list[dict]:
    """Major trades grouped by listing (symbol): one block per company on the exchange."""
    if not DATABASE_PATH.exists():
        return []

    where_clause = "WHERE report_date = ?" if as_of_date else ""
    parameters = (as_of_date.isoformat(),) if as_of_date else ()
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT
                symbol_code,
                symbol_name,
                investor_name,
                trade_type,
                quantity,
                market_pct
            FROM major_activity
            {where_clause}
            ORDER BY sort_order ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    groups_by_symbol: dict[str, dict] = {}
    group_order: list[str] = []

    for symbol_code, symbol_name, investor_name, trade_type, quantity, market_pct in rows:
        if symbol_code not in groups_by_symbol:
            groups_by_symbol[symbol_code] = {
                "symbol": symbol_code,
                "company": symbol_name,
                "rows": [],
            }
            group_order.append(symbol_code)

        groups_by_symbol[symbol_code]["rows"].append(
            {
                "investor_type": investor_name,
                "trade_type": "شراء" if trade_type == "buy" else "بيع",
                "type_class": trade_type,
                "quantity": f"{quantity:,}",
                "avg_price": "–",
                "value": "–",
                "market_pct": f"{market_pct:.2f}%",
            }
        )

    return [groups_by_symbol[s] for s in group_order]


def load_insider_trades(as_of_date: date | None = None) -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    where_clause = "WHERE report_date = ?" if as_of_date else ""
    parameters = (as_of_date.isoformat(),) if as_of_date else ()
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT symbol_code, symbol_name, insider_name, trade_side, quantity
            FROM insider_trades
            {where_clause}
            ORDER BY sort_order ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    return [
        {
            "symbol": symbol_code,
            "company": symbol_name,
            "insider": insider_name,
            "trade_type": "شراء" if trade_side == "buy" else "بيع",
            "type_class": trade_side,
            "quantity": f"{quantity:,}",
        }
        for symbol_code, symbol_name, insider_name, trade_side, quantity in rows
    ]


def parse_earnings_quarter_lines(text: str) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if "," in line:
            label_part, price_part = line.split(",", 1)
        elif "\t" in line:
            label_part, price_part = line.split("\t", 1)
        else:
            continue
        label = label_part.strip()
        try:
            price = float(price_part.strip().replace(",", "").replace("٬", ""))
        except ValueError:
            continue
        if label:
            rows.append((label, price))
    return rows


def save_earnings_quarters(rows: list[tuple[str, float]]) -> int:
    with _connect() as connection:
        connection.execute(f"DELETE FROM {EARNINGS_QUARTERS_TABLE}")
        if not rows:
            return 0
        connection.executemany(
            f"""
            INSERT INTO {EARNINGS_QUARTERS_TABLE} (sort_order, label, price)
            VALUES (?, ?, ?)
            """,
            [
                (index, label, float(price))
                for index, (label, price) in enumerate(rows)
            ],
        )
    return len(rows)


def load_earnings_quarters() -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT label, price
            FROM {EARNINGS_QUARTERS_TABLE}
            ORDER BY sort_order ASC
            """
        )
        return [
            {"label": str(label), "price": float(price)}
            for label, price in cursor.fetchall()
        ]


def parse_symbol_company_lines(text: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if "," in line:
            symbol_part, name_part = line.split(",", 1)
        elif "\t" in line:
            symbol_part, name_part = line.split("\t", 1)
        else:
            continue
        symbol = symbol_part.strip()
        company_name = name_part.strip()
        if symbol and company_name:
            rows.append((symbol, company_name))
    return rows


def save_listed_companies(rows: list[tuple[str, str]]) -> int:
    with _connect() as connection:
        connection.execute(f"DELETE FROM {LISTED_COMPANIES_TABLE}")
        if not rows:
            return 0
        connection.executemany(
            f"""
            INSERT INTO {LISTED_COMPANIES_TABLE} (symbol, company_name)
            VALUES (?, ?)
            """,
            rows,
        )
    return len(rows)


def load_listed_companies() -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT symbol, company_name
            FROM {LISTED_COMPANIES_TABLE}
            ORDER BY symbol ASC
            """
        )
        return [
            {"symbol": str(sym), "company_name": str(name)}
            for sym, name in cursor.fetchall()
        ]


def load_upcoming_events(as_of_date: date | None = None) -> list[dict]:
    if not DATABASE_PATH.exists():
        return []

    where_clause = "WHERE display_flag = 'Y'"
    parameters = ()
    if as_of_date:
        where_clause += " AND event_date = ?"
        parameters = (as_of_date.isoformat(),)
    with _connect() as connection:
        cursor = connection.execute(
            f"""
            SELECT event_date, news_time, news_title, news_theme, comp_code, news_comp
            FROM upcoming_events
            {where_clause}
            ORDER BY event_date ASC, sort_order ASC
            """,
            parameters,
        )
        rows = cursor.fetchall()

    return [
        {
            "day": _format_arabic_day(event_date),
            "month": _format_arabic_month(event_date),
            "title": news_title,
            "subtitle": _build_event_subtitle(news_time, news_comp, news_theme, comp_code),
            "time": news_time,
            "company": _build_event_company(news_comp, comp_code),
            "detail": news_title or news_theme,
        }
        for event_date, news_time, news_title, news_theme, comp_code, news_comp in rows
    ]


def build_market_chart(points: list[dict] | None = None) -> dict:
    chart_points = points if points is not None else load_qe20_time_level()
    if not chart_points:
        return FALLBACK_MARKET_CHART

    values = [point["index"] for point in chart_points]
    min_value = min(values)
    max_value = max(values)
    value_range = max(max_value - min_value, 1)

    x_start, x_end = 0, 1000
    y_top, y_bottom = 24, 136
    last_index = max(len(chart_points) - 1, 1)

    coordinates = []
    for index, point in enumerate(chart_points):
        x = x_start + ((x_end - x_start) * index / last_index)
        y = y_bottom - ((point["index"] - min_value) / value_range * (y_bottom - y_top))
        coordinates.append((round(x, 2), round(y, 2)))

    line_path = " ".join(
        f"{'M' if index == 0 else 'L'}{x} {y}"
        for index, (x, y) in enumerate(coordinates)
    )
    area_path = f"{line_path} L{x_end} 160 L{x_start} 160 Z"

    return {
        "labels": _build_time_labels(chart_points),
        "y_labels": _build_y_labels(min_value, max_value),
        "line_path": line_path,
        "area_path": area_path,
    }


def _build_time_labels(points: list[dict], count: int = 8) -> list[str]:
    if len(points) <= count:
        return [_format_time_label(point["time"]) for point in points]

    last_index = len(points) - 1
    indexes = [round(last_index * index / (count - 1)) for index in range(count)]
    return [_format_time_label(points[index]["time"]) for index in indexes]


def _format_time_label(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return value

    hour = parsed.hour - 12 if parsed.hour > 12 else parsed.hour
    return f"{hour}:{parsed.minute:02d}"


def _build_y_labels(min_value: float, max_value: float, count: int = 7) -> list[str]:
    if count <= 1:
        return [f"{max_value:,.0f}"]

    step = (max_value - min_value) / (count - 1)
    return [f"{max_value - (step * index):,.0f}" for index in range(count)]


def _format_decimal(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _format_signed_decimal(value: float, decimal_places: int = 4) -> str:
    sign = "+" if value > 0 else ""
    formatted = f"{value:.{decimal_places}f}".rstrip("0").rstrip(".")
    return f"{sign}{formatted}"


def _format_signed_fixed(value: float, decimal_places: int) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimal_places}f}"


def _format_pct(value: float, signed: bool = False) -> str:
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.3f}%"


def _format_millions(value: float) -> str:
    return f"{value / 1_000_000:.1f} م"


def _format_net_millions(value: float) -> str:
    if value == 0:
        return "لا يوجد"

    sign = "+" if value > 0 else "-"
    return f"{sign}{abs(value) / 1_000_000:.2f} م"


def _format_arabic_day(value: str) -> str:
    try:
        return str(date.fromisoformat(value).day)
    except ValueError:
        return value


def _format_arabic_month(value: str) -> str:
    month_names = {
        1: "يناير",
        2: "فبراير",
        3: "مارس",
        4: "أبريل",
        5: "مايو",
        6: "يونيو",
        7: "يوليو",
        8: "أغسطس",
        9: "سبتمبر",
        10: "أكتوبر",
        11: "نوفمبر",
        12: "ديسمبر",
    }
    try:
        return month_names[date.fromisoformat(value).month]
    except (KeyError, ValueError):
        return ""


def _build_event_subtitle(
    news_time: str,
    news_comp: str,
    news_theme: str,
    comp_code: str,
) -> str:
    company = _build_event_company(news_comp, comp_code)
    return " · ".join(part for part in [news_time, company, news_theme] if part)


def _build_event_company(news_comp: str, comp_code: str) -> str:
    return f"{news_comp} ({comp_code})" if news_comp and comp_code else news_comp or comp_code


def _bar_width(value: float, max_value: float) -> int:
    if value <= 0:
        return 0

    return max(4, int(round(value / max_value * 100)))


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

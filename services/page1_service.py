from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import requests


QE20_TIME_LEVEL_URL = "https://www.qe.com.qa/wp/mw/data/QE20_TimeLevel.txt"
MARKET_INDEX_URL = "https://www.qe.com.qa/wp/mw/data/Index.txt"
TOP5_URL_TEMPLATE = "https://www.qe.com.qa/wp/trading_report_data/{year}/{month}/{day}/Top5.txt"
DATABASE_DIR = Path("database")
DATABASE_PATH = DATABASE_DIR / "finance_report.sqlite3"
TOP5_JSON_PREFIX = "for(;;);"
QE20_TOPIC = "QE20/ID"
MARKET_INDEX_TABLE = "market_index"
QE20_TIME_LEVEL_TABLE = "qe20_time_level"
TOP_MOVERS_TABLE = "top_movers"

FALLBACK_MARKET_CHART = {
    "labels": ["9:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "1:00"],
    "y_labels": ["10,500", "10,490", "10,480", "10,470", "10,460", "10,450", "10,440"],
    "line_path": "M0 132 C55 122 81 126 109 125 C157 124 195 100 257 95 C311 88 341 104 389 96 C443 87 470 67 508 72 C557 78 573 68 617 65 C670 62 689 79 734 69 C787 55 815 44 870 46 C918 49 942 42 1000 40",
    "area_path": "M0 132 C55 122 81 126 109 125 C157 124 195 100 257 95 C311 88 341 104 389 96 C443 87 470 67 508 72 C557 78 573 68 617 65 C670 62 689 79 734 69 C787 55 815 44 870 46 C918 49 942 42 1000 40 L1000 160 L0 160 Z",
}


def fetch_qe20_time_level() -> list[dict]:
    response = requests.get(QE20_TIME_LEVEL_URL, timeout=20)
    response.raise_for_status()
    return response.json()


def fetch_market_index() -> list[dict]:
    response = requests.get(MARKET_INDEX_URL, timeout=20)
    response.raise_for_status()
    payload = response.json()
    return payload.get("rows", [])


def fetch_top_movers(
    as_of_date: date | None = None,
    max_lookback_days: int = 30,
) -> tuple[str, list[dict]]:
    current_date = as_of_date or date.today()

    for _ in range(max_lookback_days + 1):
        url = TOP5_URL_TEMPLATE.format(
            year=current_date.strftime("%Y"),
            month=current_date.strftime("%m"),
            day=current_date.strftime("%d"),
        )
        response = requests.get(url, timeout=20)
        if response.status_code == 404:
            current_date -= timedelta(days=1)
            continue

        response.raise_for_status()
        return current_date.isoformat(), _parse_top5_response(response.content)

    raise ValueError(f"No Top5 feed found in the last {max_lookback_days + 1} days")


def _parse_top5_response(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig").strip()
    if text.startswith(TOP5_JSON_PREFIX):
        text = text[len(TOP5_JSON_PREFIX):]

    return json.loads(text)


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
    return connection


def save_market_index(rows: list[dict]) -> int:
    clean_rows = [
        (
            str(row["Topic"]),
            json.dumps(row, ensure_ascii=False),
            int(row.get("Order", index)),
        )
        for index, row in enumerate(rows)
        if row.get("Topic")
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
        if "Time" in row and "Index" in row
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
        if "DATA_TYPE" in row and "SYMBOL_CODE" in row
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


def run_update_jobs() -> dict:
    market_index_rows = fetch_market_index()
    market_index_count = save_market_index(market_index_rows)
    market_index_date = _get_market_index_date(market_index_rows)
    market_index_last_update = _get_market_index_last_update(market_index_rows)

    chart_rows = fetch_qe20_time_level()
    chart_date_from, chart_date_to = _get_chart_date_range(chart_rows)
    chart_data_date = _get_chart_data_date(chart_rows)
    chart_count = save_qe20_time_level(chart_rows)

    top_movers_date, top_mover_rows = fetch_top_movers()
    top_movers_count = save_top_movers(top_movers_date, top_mover_rows)

    result = {
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
        "database_path": str(DATABASE_PATH),
    }
    result["log_lines"] = _build_run_job_log_lines(result)
    return result


def _build_run_job_log_lines(result: dict) -> list[str]:
    return [
        "Run Job completed successfully.",
        f"Database: {result['database_path']}",
        "APIs processed: 3",
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
    ]


def _get_market_index_date(rows: list[dict]) -> str:
    row = _get_preferred_market_index_row(rows)
    return str(row.get("Date", ""))


def _get_market_index_last_update(rows: list[dict]) -> str:
    row = _get_preferred_market_index_row(rows)
    return str(row.get("LastUpdate", ""))


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
        "traded_value": f"م {traded_value / 1_000_000:.1f} ر.ق",
        "traded_volume": f"م {traded_volume / 1_000_000:.1f} سهم",
        "commentary": (
            f"أغلق مؤشر QE {direction_text} عند {last_price:,.2f} "
            f"بتغيير {_format_signed_decimal(change)} نقطة "
            f"({_format_signed_fixed(percent_change, 2)}%)، وبلغت قيمة التداول "
            f"{traded_value:,.0f} ر.ق على حجم {traded_volume:,.0f} سهم."
        ),
    }


def load_top_movers() -> dict:
    if not DATABASE_PATH.exists():
        return {}

    with _connect() as connection:
        cursor = connection.execute(
            """
            SELECT data_type, symbol_code, traded_value, close_price
            FROM top_movers
            ORDER BY sort_order ASC
            """
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
    return f"م{value / 1_000_000:.1f}"


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

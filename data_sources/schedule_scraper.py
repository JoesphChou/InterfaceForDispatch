from __future__ import annotations
from bs4 import BeautifulSoup
import re, urllib3, time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
from logging_utils import get_logger
logger = get_logger(__name__)

"""schedule_scraper.py

A standalone helper that scrapes Dragon Steel MES “2138” (schedule chart) and “2137”
(status page) and classifies each record into **past**, **current** or **future**
relative to *now*.

The implementation is refactored from the former ``scrapy_schedule`` routine in
``main.py`` so that the UI layer no longer owns heavy parsing logic.

Usage
-----
>>> from schedule_scraper import scrape_schedule
>>> past, current, future = scrape_schedule()  # returns three DataFrames

Each DataFrame columns
----------------------
* 開始時間 (datetime64[ns])
* 結束時間 (datetime64[ns])
* 爐號       (str)
* 製程       (str) – EAFA / EAFB / LF1-1 / LF1-2
* 製程狀態   (only for ``current``)

The function purposefully keeps the original heuristics for de‑duplication,
cross‑day correction and coordinate‑based sorting so that the UI’s downstream
logic remains unchanged.
"""
# 公開介面宣告：意思是當其它 使用用【from schedule_scraper import *】 時，
# 只會匯入scrape_schedule
__all__ = ["scrape_schedule"]

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
URL_2138 = "http://w3mes.dscsc.dragonsteel.com.tw/2138.aspx"
URL_2137 = "http://w3mes.dscsc.dragonsteel.com.tw/2137.aspx"

# Y‑axis pixel ranges of the bitmap‐map that identify each process row on 2138
_PROCESS_Y_RANGES: Dict[str, Tuple[int, int]] = {
    "EAFA": (179, 197),
    "EAFB": (217, 235),
    "LF1-1": (250, 268),
    "LF1-2": (286, 304),
}

# Title patterns used by MES page when hovering the area map.
_TIME_PATTERNS: Dict[str, str] = {
    proc: rf"{proc}時間:\s*(\d{{2}}:\d{{2}}:\d{{2}})\s*~\s*(\d{{2}}:\d{{2}}:\d{{2}})"  # noqa: E501
    for proc in _PROCESS_Y_RANGES
}

# 建立一個全域變數(ulrlib3.PoolManger 的實例)，用來管理HTTP連線，可重複使用連線(比每次都重新開socket 快很多),
# retries=False ->不自動重試, timeout=5.0 ->每次請求的逾時設定是5秒
_POOL = urllib3.PoolManager(retries=False, timeout=5.0)

# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def scrape_schedule(
    *,
    now: datetime or None = None,
    pool: urllib3.PoolManager or None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """Fetch both pages and return *(past_df, current_df, future_df, status)*.

    Parameters
    ----------
    now: datetime | None, default ``datetime.now()``
        Point‑in‑time that defines the three categories. Allows deterministic
        unit‑testing.
    pool: urllib3.PoolManager | None
        Inject an existing connection pool; falls back to a module‑level one.
    """
    if now is None:
        now = pd.Timestamp.now()
    pool = pool or _POOL

    # ------------------------------------------------------------------
    # 1. Schedule rectangles from 2138 -------------------------------------------------
    # ------------------------------------------------------------------
    soup_2138 = _fetch_soup(URL_2138, pool)
    if soup_2138 is None:
        # 伺服器回 500 → 先以空的 DataFrame 代替
        past_df = pd.DataFrame(columns=["Group1", "Group2", "value", "timestamp"])
        current_df = pd.DataFrame(columns=["Group1", "Group2", "value", "timestamp"])
        future_df = pd.DataFrame(columns=["Group1", "Group2", "value", "timestamp"])
        status = "ERROR"
        return past_df, current_df, future_df, status

    areas = soup_2138.find_all("area")

    raw_sched: List[Tuple[int, datetime, datetime, str, str]] = []

    for area in areas:
        title = area.get("title", "")
        furnace_match = re.search(r"爐號[＝>:\s]*([A-Za-z0-9]+)", title)
        furnace_id = furnace_match.group(1) if furnace_match else "未知"

        coords = [int(x) for x in re.findall(r"\d+", area.get("coords", ""))]
        if len(coords) < 2:
            continue
        x_coord, y_coord = coords[0], coords[1]

        process_type = _infer_process_type(y_coord)
        if process_type is None or process_type not in title:
            continue

        m = re.search(_TIME_PATTERNS[process_type], title)
        if not m:
            continue
        start_ts, end_ts = m.groups()

        today = now.date().isoformat()
        start = pd.to_datetime(f"{today} {start_ts}")
        end = pd.to_datetime(f"{today} {end_ts}")

        raw_sched.append((x_coord, start, end, furnace_id, process_type))

    # Sort EAFA/EAFB together as EAF but preserve start time for tie‑breakers.
    sorted_sched = _sort_schedules(raw_sched)
    filtered_sched = _deduplicate(sorted_sched)
    filtered_sched = _adjust_cross_day(filtered_sched, now)

    # ------------------------------------------------------------------
    # 2. Process status from 2137 ------------------------------------------------------
    # ------------------------------------------------------------------
    soup_2137 = _fetch_soup(URL_2137, pool)
    status_map = {
        "EAFA": _get_status(soup_2137, "lbl_eafa_period"),
        "EAFB": _get_status(soup_2137, "lbl_eafb_period"),
        "LF1-1": _get_status(soup_2137, "lbl_lf11_period"),
        "LF1-2": _get_status(soup_2137, "lbl_lf12_period"),
    }

    # ------------------------------------------------------------------
    # 3. Classification -------------------------------------------------------------
    # ------------------------------------------------------------------
    seen: Dict[str, Dict[str, set[str]]] = {
        cat: {proc: set() for proc in _PROCESS_Y_RANGES} for cat in ("past", "current", "future")
    }
    past, current, future = [], [], []

    for x, start, end, furnace, proc in filtered_sched:
        if end < now:
            bucket = past
            category = "past"
        elif start > now:
            bucket = future
            category = "future"
        else:
            bucket = current
            category = "current"

        if furnace in seen[category][proc]:
            continue  # per‑process de‑duplication
        seen[category][proc].add(furnace)

        record = [start, end, furnace, proc]
        if category == "current":
            record.append(status_map.get(proc, "未知"))
        bucket.append(record)

    past_df = pd.DataFrame(past, columns=["開始時間", "結束時間", "爐號", "製程"])
    current_df = pd.DataFrame(current, columns=["開始時間", "結束時間", "爐號", "製程", "製程狀態"])
    future_df = pd.DataFrame(future, columns=["開始時間", "結束時間", "爐號", "製程"])
    status = "OK"
    return past_df, current_df, future_df, status

# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------

def _fetch_soup(url: str, pool: urllib3.PoolManager, retries: int = 2, delay: float = 2.0):
    """
    以 urllib3 取得網頁並回傳 BeautifulSoup.
    若 HTTP status 不是 200，則記錄錯誤，但不主動丟出 RuntimeError.
    """
    for attempt in range(1, retries + 1):
        r = pool.request("GET", url)
        if r.status == 200:
            # 記錄錯誤，但不丟例外
            return BeautifulSoup(r.data, "html.parser")
        logger.warning(f"第 {attempt} 次 GET {url} 失敗: HTTP {r.status}")
        time.sleep(delay)
    # 最後一次仍失敗
    logger.error(f"多次重試後仍無法 GET {url}，回傳 None")
    return None

def _infer_process_type(y: int) -> str or None:
    for proc, (lo, hi) in _PROCESS_Y_RANGES.items():
        if lo <= y <= hi:
            return proc
    return None


def _sort_schedules(raw: List[Tuple[int, datetime, datetime, str, str]]):
    """Sort by virtual sort_group then x‑axis coordinate then start time."""
    def sort_group(proc: str) -> str:
        return "EAF" if proc in ("EAFA", "EAFB") else proc

    return sorted(
        raw,
        key=lambda t: (sort_group(t[4]), t[0], t[1]),
    )


def _deduplicate(sorted_sched):
    """Remove duplicates (same furnace id within the same process)."""
    filtered: List[Tuple[int, datetime, datetime, str, str]] = []
    for rec in sorted_sched:
        _, _, _, furnace, proc = rec
        existing_ids = {r[3] for r in filtered if r[4] == proc}
        if furnace not in existing_ids:
            filtered.append(rec)
    return filtered

def _adjust_cross_day(records, now: datetime):
    """Apply original cross‑day heuristics in‑place and return a new list."""
    adjusted = list(records)

    def unify(proc):
        return "EAF" if proc in ("EAFA", "EAFB") else proc

    for i, (x, start, end, furnace, proc) in enumerate(adjusted):
        # MES rectangles wrap at 00:00 so end might be earlier than start.
        if end < start:
            end += pd.Timedelta(days=1)

        # Special handling for early‑morning viewing window (<08:00)
        if now.time() < datetime.strptime("08:00", "%H:%M").time():
            if abs(now - start) > pd.Timedelta(hours=10):
                start -= pd.Timedelta(days=1)
                end -= pd.Timedelta(days=1)
        elif i == 0 and abs(now - start) > pd.Timedelta(hours=10):
            start += pd.Timedelta(days=1)
            end += pd.Timedelta(days=1)

        # If this process already has a previous record and ordering is weird,
        # assume wrap‑around.
        if i > 0:
            prev_proc = unify(adjusted[i - 1][4])
            if unify(proc) == prev_proc and start < adjusted[i - 1][1]:
                start += pd.Timedelta(days=1)
                end += pd.Timedelta(days=1)

        adjusted[i] = (x, start, end, furnace, proc)
    return adjusted


def _get_status(soup: BeautifulSoup, element_id: str) -> str:
    span = soup.find("span", {"id": element_id})
    return span.text.strip() if span else "未知"


if __name__ == "__main__":  # pragma: no cover
    p_df, c_df, f_df, status = scrape_schedule()
    print("Past\n", p_df.tail())
    print("Current\n", c_df)
    print("Future\n", f_df.head())
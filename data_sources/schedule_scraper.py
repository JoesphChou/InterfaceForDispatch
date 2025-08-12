"""schedule_scraper.py

A standalone helper that scrapes Dragon Steel MES “2138” (schedule chart) and “2137”
(status page) and classifies each record into **past**, **current** or **future**
relative to *now*.

The implementation is refactored from the former ``scrapy_schedule`` routine in
``main.py`` so that the UI layer no longer owns heavy parsing logic.

Usage
-----
- >>> from schedule_scraper import scrape_schedule
- >>> past, current, future = scrape_schedule()  # returns three DataFrames

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

from __future__ import annotations
from bs4 import BeautifulSoup
import re, urllib3, time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
from logging_utils import get_logger
logger = get_logger(__name__)

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
    """
    抓取並解析 MES 2138、2137 頁面，回傳「過去／目前／未來」三個 DataFrame 及狀態。

    Args:
        now (datetime | None):
            用於分類的參考時間，預設為當前時間。
        pool (urllib3.PoolManager | None):
            HTTP 連線池，預設使用模組內 _POOL。

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
            past_df: 已結束排程 (欄位: 開始時間, 結束時間, 爐號, 製程)
            current_df: 正在進行排程 (欄位: 開始時間, 結束時間, 爐號, 製程, 製程狀態)
            future_df: 尚未開始排程 (欄位: 開始時間, 結束時間, 爐號, 製程)
            status: 執行結果 "OK" 或 "ERROR"。
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
    使用 urllib3 取得網頁並解析成 BeautifulSoup。

    Args:
        url (str): 請求的網址。
        pool (urllib3.PoolManager): 連線池。
        retries (int): 重試次數，預設 2 次。
        delay (float): 重試間隔秒數，預設 2.0 秒。

    Returns:
        BeautifulSoup | None: 成功回傳解析後的 soup，否則 None。
    """
    for attempt in range(1, retries + 1):
        r = pool.request("GET", url)
        if r.status == 200:
            # Http 請成成功
            return BeautifulSoup(r.data, "html.parser")
        # logger.warning(f"第 {attempt} 次 GET {url} 失敗: HTTP {r.status}")
        time.sleep(delay)
    else:
        # 迴圈跑完都沒回傳，則記錄錯誤
        logger.error(f"多次重試後仍無法 GET {url}，回傳 None")
    return None

def _infer_process_type(y: int) -> str or None:
    """
    根據 y 座標判斷製程類型。

    Args:
        y (int): area 元素的 Y 軸 pixel 值。

    Returns:
        str | None: 對應的製程名稱（EAFA, EAFB, LF1-1, LF1-2），找不到時回傳 None。
    """
    for proc, (lo, hi) in _PROCESS_Y_RANGES.items():
        if lo <= y <= hi:
            return proc
    return None


def _sort_schedules(raw: List[Tuple[int, datetime, datetime, str, str]]):
    """
    依製程群組、X 軸座標、起始時間排序。

    Args:
        raw (List[Tuple[int, datetime, datetime, str, str]]): 原始排程列表。

    Returns:
        List[Tuple[int, datetime, datetime, str, str]]: 排序後的列表。
    """
    def sort_group(proc: str) -> str:
        return "EAF" if proc in ("EAFA", "EAFB") else proc

    return sorted(
        raw,
        key=lambda t: (sort_group(t[4]), t[0], t[1]),
    )


def _deduplicate(sorted_sched):
    """
    移除相同製程中重複的爐號記錄。

    Args:
        schedules: 已排序的排程列表。

    Returns:
        List[Tuple[int, datetime, datetime, str, str]]: 去重後的列表。
    """

    filtered: List[Tuple[int, datetime, datetime, str, str]] = []
    for rec in sorted_sched:
        _, _, _, furnace, proc = rec
        existing_ids = {r[3] for r in filtered if r[4] == proc}
        if furnace not in existing_ids:
            filtered.append(rec)
    return filtered

def _adjust_cross_day(records, now: datetime):
    """
    對跨日區間進行調整，保持排序一致。

    Args:
        records: 去重後的排程列表。
        now (datetime): 分類參考時間。

    Returns:
        List[Tuple[int, datetime, datetime, str, str]]: 調整後的列表。
    """

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
    """
    從狀態頁面取得指定製程的狀態文字。

    Args:
        soup (BeautifulSoup): 解析後的狀態頁面。
        element_id (str): HTML 頁面中 span 元素的 id。

    Returns:
        str: 取得的狀態文字，若找不到則回傳 "未知"。
    """
    span = soup.find("span", {"id": element_id})
    return span.text.strip() if span else "未知"


if __name__ == "__main__":  # pragma: no cover
    p_df, c_df, f_df, status = scrape_schedule()
    print("Past\n", p_df.tail())
    print("Current\n", c_df)
    print("Future\n", f_df.head())
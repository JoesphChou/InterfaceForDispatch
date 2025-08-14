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
import re, urllib3
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Set
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

"""
# 建立一個全域變數(ulrlib3.PoolManger 的實例)，用來管理HTTP連線，可重複使用連線(比每次都重新開socket 快很多),
  自動重試3次, timeout 改為10.0 ->每次請求的逾時設定是10秒
    total=3，最多總共重試3
    backoff_factor=1，是設定『退避等待時間』的基礎倍數。
    e.g.  1st:     不延遲
          2nd:     backoff_factor x 2^(2-1)= 2秒
          3rd:     backoff_factor x 2^(3-1)= 4秒 
    status_forelist[....]，如果伺服器回應是這幾種錯誤碼，就進行重試：
    e.g.    500 -> Internal Server Error (伺服器內部錯)
            502 -> Bad Gateway           (上游伺服器錯)
            503 -> Service Unavailable   (暫時性故障)
            504 -> Gateway Timeout       (網路逾時)
"""
_POOL = urllib3.PoolManager(retries= urllib3.util.retry.Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500,502,503,504],
    ),
    timeout=10.0)

@dataclass
class ScheduleResult:
    """封裝排程擷取結果的資料物件。

    用於隔離資料層與 UI 層的責任：
    - 資料層（scraper）只需回傳成功/失敗狀態與資料，不直接介入 UI。
    - UI 層根據 ok 與 reason 決定提示與後續行為。

    Attributes:
        ok (bool): 是否成功取得並解析資料。True 表示成功，False 表示失敗。
        past (pd.DataFrame): 過去排程資料表。成功時為解析結果，失敗時為空表。
        current (pd.DataFrame): 目前排程資料表。成功時為解析結果，失敗時為空表。
        future (pd.DataFrame): 未來排程資料表。成功時為解析結果，失敗時為空表。
        reason (str): 若 `ok=False`，此欄位描述失敗原因（例如「連線逾時或伺服器暫時無回應」、
            「資料解析失敗」等）。成功時建議為空字串。
        fetched_at (pd.Timestamp): 建立此結果的時間戳記（本地時間）。
            預設為建立物件當下（`pd.Timestamp.now()`）。
    """
    ok: bool
    past: pd.DataFrame
    current: pd.DataFrame
    future: pd.DataFrame
    reason: str = ""
    fetched_at: pd.Timestamp = field(default_factory=pd.Timestamp.now)

def _empty_df() -> pd.DataFrame:
    """回傳具有預期欄位的空白 DataFrame。

    回傳的欄位應與正常解析後的 DataFrame 一致，以降低 UI 或後續處理出現
    KeyError 的風險。若暫時未定義欄位，也可先回傳完全空表，日後再補齊。

    Returns:
        pd.DataFrame: 空的 DataFrame。建議包含預期欄位
            （例如：["開始時間", "結束時間", "製程", "爐號", "製程狀態"]）。
    """
    return pd.DataFrame()

# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------

def scrape_schedule(
    *,
    now: datetime or None = None,
    pool: urllib3.PoolManager or None = None,
) -> ScheduleResult:
    """
    抓取並解析 MES 2138、2137 頁面，回傳「過去／目前／未來」三個 DataFrame 及狀態。

    流程包含：
    1) 透過 _fetch_soup() 抓取頁面 HTML（重試/逾時由 PoolManager 的 Retry/timeout 管理）。
    2) 解析表格與時間區段資訊，整理為統一的列格式。
    3) 套用跨日/排序等 heuristics，並依 now 切分為 past/current/future 三張表。

    設計原則：
    - 本函式**不拋出 UI 相關例外**；失敗時回傳 `ScheduleResult(ok=False, reason=...)`，
      三張表皆為空表。UI 層可依據 ok 與 reason 顯示提示。
    - 連線暫時性錯誤（如逾時）會回傳 ok=False 並在 reason 說明。
    - 解析失敗（格式變更、資料不完整）會回傳 ok=False 並在 reason 說明。

    Args:
        now (datetime | None):
            用於分類的參考時間，預設為當前時間。
        pool (Optional[urllib3.PoolManager]): 自訂連線池；若為 None，將使用模組內建的
        `PoolManager(retries=Retry(...), timeout=...)`。保留參數可利於測試注入替身(mock)。

    Returns:
        ScheduleResult: 包含三張 DataFrame（past/current/future）、狀態旗標 ok
                        與失敗原因 `reason`的結果物件。
    """
    if now is None:
        now = pd.Timestamp.now()
    pool = pool or _POOL

    # ------------------------------------------------------------------
    # 1. Schedule rectangles from 2138 -------------------------------------------------
    # ------------------------------------------------------------------
    soup_2138 = _fetch_soup(URL_2138, pool)
    soup_2137 = _fetch_soup(URL_2137, pool)

    if soup_2138 is None or soup_2137 is None:
        return ScheduleResult(
            ok = False,
            past = _empty_df(),
            current = _empty_df(),
            future = _empty_df(),
            reason="連線逾時或伺服器暫時無回應",
        )
    try:
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

        status_map = {
            "EAFA": _get_status(soup_2137, "lbl_eafa_period"),
            "EAFB": _get_status(soup_2137, "lbl_eafb_period"),
            "LF1-1": _get_status(soup_2137, "lbl_lf11_period"),
            "LF1-2": _get_status(soup_2137, "lbl_lf12_period"),
        }

        # ------------------------------------------------------------------
        # 3. Classification -------------------------------------------------------------
        # ------------------------------------------------------------------
        seen: Dict[str, Dict[str, Set[str]]] = {
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

        #return past_df, current_df, future_df, status
        return ScheduleResult(
            ok = True,
            past = past_df,
            current = current_df,
            future = future_df,
            reason = "",
        )
    except Exception:
        logger.exception("解析排程資料失敗")
        return ScheduleResult(
            ok=False,
            past=_empty_df(),
            current=_empty_df(),
            future=_empty_df(),
            reason="資料解析失敗",
        )
# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------

def _fetch_soup(url: str, pool: urllib3.PoolManager) -> Optional[BeautifulSoup]:
    """以 urllib3.PoolManager 取得 HTML 並回傳 BeautifulSoup 物件。

    重試（Retry）與逾時（timeout）由傳入的 pool 物件設定管理；
    本函式不做手動重試。若非 200 或發生例外，回傳 None。

    Args:
        url (str): 目標頁面 URL。
        pool (urllib3.PoolManager): 已帶有 Retry/timeout 設定的連線池。

    Returns:
        Optional[BeautifulSoup]: 成功時的 soup 物件；失敗（非 200 或例外）時回傳 None。

    Notes:
        - 建議在呼叫端統一處理「抓取失敗」的情況，避免在多層重複 UI 呈現。
        - 若需記錄更細的 retry 訊息，請在建立 pool 時配置 Retry 或於上層統一觀測。
    """
    try:
        r = pool.request("GET", url)  # 重試與 timeout 由 pool 決定
        if r.status == 200:
            return BeautifulSoup(r.data, "html.parser")
        else:
            logger.warning(f"GET {url} 回應非 200：HTTP {r.status}")
            return None
    except urllib3.exceptions.HTTPError as e:
        # 包含連線錯誤、讀取逾時等都屬於此層級
        logger.error(f"抓取 {url} 發生 HTTP 錯誤：{e}")
        return None
    except Exception as e:
        # 保底：避免非預期例外中斷流程
        logger.exception(f"抓取 {url} 發生未預期錯誤：{e}")
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
    """移除同一製程 (process) 中重複的爐號 (furnace) 紀錄，只保留第一筆出現的紀錄。

    Parameters
    ----------
    sorted_sched : list[tuple[int, datetime, datetime, str, str]]
        已經依「製程群組 → X 座標 → 開始時間」排序過的排程紀錄清單。
        每筆紀錄的結構為：
            (x_coord, start_time, end_time, furnace_id, process_type)

    Returns
    -------
    list[tuple[int, datetime, datetime, str, str]]
        去重後的排程清單。於同一製程內，若爐號重複出現，僅保留第一筆出現的紀錄。

    Notes
    -----
    - 由於輸入 sorted_sched 已事先排序，因此「第一筆」代表在該製程視角下最靠前
      或最早的紀錄（取決於排序鍵）。
    - 此設計用於避免 UI 在顯示 MES 圖表資料時，因同爐號重複出現造成的排程混亂。
    """

    filtered: List[Tuple[int, datetime, datetime, str, str]] = []
    for rec in sorted_sched:
        _, _, _, furnace, proc = rec
        existing_ids = {r[3] for r in filtered if r[4] == proc}
        if furnace not in existing_ids:
            filtered.append(rec)
    return filtered

def _adjust_cross_day(records, now: datetime):
    """調整跨日情境下的排程時間，使其在 X 軸與實際時間對齊。

     常見情境：
         - 當某些排程的開始/結束時間跨越午夜，需平移至正確日界線。
         - 在第一筆資料且與 now 相差過大時，進行合理的加/減日調整。
    Args:
        records: 去重後的排程列表。
        now (datetime): 分類參考時間。

    Returns:
        list[tuple[int, pd.Timestamp, pd.Timestamp, str, str]]: 完成跨日對齊後的排程清單。

    Notes:
        - 若 now 與 start/end 的 tz 屬性不一致（一個 aware、一個 naive），
          pandas 會丟出 TypeError。請在呼叫前先統一型別。
        - 具體判斷閾值（例如「相差 > 10 小時則調整」）可視現場資料特性調整。
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
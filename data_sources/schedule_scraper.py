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
from typing import Optional, Dict, List, Tuple, Set, Sequence
import pandas as pd
from datetime import datetime
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
URL_2133 = "http://w3mes.dscsc.dragonsteel.com.tw/2133.aspx"
URL_2143 = "http://w3mes.dscsc.dragonsteel.com.tw/2143.aspx"

# Y‑axis pixel ranges of the bitmap‐map that identify each process row on 2138
_PROCESS_Y_RANGES: Dict[str, Tuple[int, int]] = {
    "EAFA": (179, 197),
    "EAFB": (217, 235),
    "LF1-1": (250, 268),
    "LF1-2": (286, 304),
}

# Y‑axis pixel ranges of the bitmap‐map that identify each process row on 2133
_FIXED_LANES = {
    "LF1": {"min": 281, "max": 309},
    "LF2": {"min": 322, "max": 348},
    "SCC1": {"min": 487, "max": 513},
    "SCC2": {"min": 528, "max": 554},
    "SCC3": {"min": 569, "max": 596},
}

# Title patterns used by MES page when hovering the area map.
_TIME_PATTERNS: Dict[str, str] = {
    proc: rf"{proc}時間:\s*(\d{{2}}:\d{{2}}:\d{{2}})\s*~\s*(\d{{2}}:\d{{2}}:\d{{2}})"  # noqa: E501
    for proc in _PROCESS_Y_RANGES
}

# 2133：title 辨識
_RE_SCC = re.compile(r"SCC開始時間\s*:\s*(\d{2}:\d{2}:\d{2}).*?SCC結束時間\s*:\s*(\d{2}:\d{2}:\d{2})", re.S)
_RE_BOF = re.compile(r"BOF開始時間\s*:\s*(\d{2}:\d{2}:\d{2}).*?BOF結束時間\s*:\s*(\d{2}:\d{2}:\d{2})", re.S)
_RE_STEP = re.compile(r"STEP\s*(\d+)\s*開始時間\s*:\s*(\d{2}:\d{2}:\d{2}).*?STEP\s*\1\s*結束時間\s*:\s*(\d{2}:\d{2}:\d{2})", re.S)

# y 車道容忍值（畫素）
_SCC_Y_TOL = 6

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

def scrape_schedule_v2(
    *,
    now: datetime = None,
    pool: urllib3.PoolManager  = None,
    include_2133_planned: bool = True,
    include_2133_actual: bool = True,
    include_2143_status: bool = True,
) -> ScheduleResult:
    """
    新版整合爬蟲：
    1) 先調用原本 scrape_schedule() 取得 2138/2137 的 past/current/future。
    2) 視參數決定是否整併 2133 表定(由 SCC 校準 x→time) 與 2133 實際(以 STEPn + y 車道)。
       - 合併規則：同爐號+同製程(LF1/LF2) 且時間重疊時，'實際' 優先於 '表定'。
    3) 視參數決定是否查 2143 即時狀態，覆寫 current 中 LF 的「製程狀態」欄。
    4) 回傳與舊版相同的 ScheduleResult（不新增欄位），以確保 UI 不需改動。
    """
    now_ts = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    pool = pool or _POOL

    # --- 1) 先拿舊版主結果（2138/2137） ---
    base = scrape_schedule(now=now_ts, pool=pool)
    if not base.ok:
        return base

    past_df   = base.past.copy()
    current_df= base.current.copy()
    future_df = base.future.copy()

    # 正規化欄位集合（避免 concat 出現缺欄）
    def _ensure_cols(df, cols):
        for c in cols:
            if c not in df.columns:
                df[c] = "" if c == "製程狀態" else pd.NaT if "時間" in c else ""
        return df[cols]

    cols_past_future = ["開始時間","結束時間","爐號","製程"]
    cols_current     = ["開始時間","結束時間","爐號","製程","製程狀態"]

    past_df    = _ensure_cols(past_df, cols_past_future)
    current_df = _ensure_cols(current_df, cols_current)
    future_df  = _ensure_cols(future_df, cols_past_future)

    # --- 合併小工具 ---
    def _concat_bucket(dst, src, is_current=False):
        if src is None or src.empty:
            return dst
        need_cols = cols_current if is_current else cols_past_future
        src2 = _ensure_cols(src.copy(), need_cols)
        out = pd.concat([dst, src2], ignore_index=True)
        # 排序 & 去重（沿用你既有的慣例：開始時間優先）
        out = out.sort_values(["開始時間","結束時間","爐號","製程"], kind="mergesort", ignore_index=True)
        return out

    def _overlap(a0,a1,b0,b1) -> bool:
        return (a0 <= b1) and (b0 <= a1)

    # --- 2) 整併 2133（表定/實際） ---
    # 2-1) 2133 表定
    if include_2133_planned:
        res_plan = scrape_schedule_2133_planned(now=now_ts, pool=pool)
        if res_plan.ok:
            past_df   = _concat_bucket(past_df,   res_plan.past,   is_current=False)
            current_df= _concat_bucket(current_df,res_plan.current,is_current=True)
            future_df = _concat_bucket(future_df, res_plan.future, is_current=False)

    # 2-2) 2133 實際（優先權高於表定：同爐號+同製程且時間重疊時，移除表定）
    if include_2133_actual:
        res_act = scrape_schedule_2133_actual(now=now_ts, pool=pool)
        if res_act.ok:
            # 過去/未來：直接合併
            past_df   = _concat_bucket(past_df,   res_act.past,   is_current=False)
            future_df = _concat_bucket(future_df, res_act.future, is_current=False)

            # current：先把既有 current 中「表定」且與「實際」重疊的 LF 列移除，再塞入 res_act.current
            if not res_act.current.empty:
                act_rows = res_act.current.copy()
                act_rows = _ensure_cols(act_rows, cols_current)

                # 標記需要移除的 index（被實際覆蓋的表定 LF）
                to_drop = set()
                if not current_df.empty:
                    # 僅針對 LF1/LF2 的列（避免影響非 LF 製程）
                    mask_lf = current_df["製程"].astype(str).str.upper().isin(["LF1","LF2"])
                    for i, crow in current_df[mask_lf].iterrows():
                        c_is_planned = str(crow.get("製程狀態","")) == "表定"
                        if not c_is_planned:
                            continue
                        for _, arow in act_rows.iterrows():
                            same_furnace = str(crow["爐號"]) == str(arow["爐號"])
                            same_proc    = str(crow["製程"]) == str(arow["製程"])
                            if not (same_furnace and same_proc):
                                continue
                            if _overlap(crow["開始時間"], crow["結束時間"], arow["開始時間"], arow["結束時間"]):
                                to_drop.add(i); break
                if to_drop:
                    current_df = current_df.drop(index=list(to_drop)).reset_index(drop=True)

                # 合併實際列
                current_df = _concat_bucket(current_df, act_rows, is_current=True)

    # --- 3) 2143 狀態覆寫（可選）：只更新 current 中 LF1/LF2 的「製程狀態」 ---
    if include_2143_status and not current_df.empty:
        stat = scrape_lf_status_2143(pool=pool)
        if isinstance(stat, dict) and stat.get("ok"):
            # 建一個 {LF1/2: 狀態字串}，沒有就不覆寫
            lane_status = {
                "LF1": stat.get("LF1",{}).get("生產狀態","") or "",
                "LF2": stat.get("LF2",{}).get("生產狀態","") or "",
            }
            if any(lane_status.values()):
                # 只針對 current 的 LF 列：把空白或「表定/實際」改成「實際｜<狀態>」或直接覆寫狀態
                def _merge_status(old, lane):
                    st = lane_status.get(lane, "")
                    if not st:
                        return old
                    old = str(old or "").strip()
                    if old in ("", "表定", "實際"):
                        # 保留語意：若本來是實際，變 "實際｜狀態"
                        return ("實際｜" + st) if old == "實際" else st
                    # 已有內容就附加
                    return f"{old}｜{st}"

                mask_lf = current_df["製程"].astype(str).str.upper().isin(["LF1","LF2"])
                current_df.loc[mask_lf, "製程狀態"] = [
                    _merge_status(old, lane)
                    for old, lane in zip(current_df.loc[mask_lf, "製程狀態"], current_df.loc[mask_lf, "製程"])
                ]

    # --- 4) 最終欄位校正 & 排序 ---
    past_df    = _ensure_cols(past_df, cols_past_future).sort_values(["開始時間","結束時間","爐號","製程"], ignore_index=True)
    current_df = _ensure_cols(current_df, cols_current).sort_values(["開始時間","結束時間","爐號","製程"], ignore_index=True)
    future_df  = _ensure_cols(future_df, cols_past_future).sort_values(["開始時間","結束時間","爐號","製程"], ignore_index=True)

    return ScheduleResult(
        ok=True,
        past=past_df,
        current=current_df,
        future=future_df,
        reason=""
    )

# === 追加：2133 表定（SCC 校準 x→time，對 LF 的 x1/x2 投影） ============
def scrape_schedule_2133_planned(*, now: Optional[datetime] = None,
                                 pool: Optional[urllib3.PoolManager]=None) -> ScheduleResult:
    """
    解析 2133 頁面「表定」排程：
    先在 SCC1~SCC3 車道收集含有「SCC開始/結束」的校準點，建立 x→time 映射，
    再將位於 LF1/LF2 車道的矩形 (x1,x2) 投影成（表定）開始/結束時間，
    並依 now 分桶為 past/current/future。

    流程
    ----
    1) 透過 _fetch_2133_areas() 讀取所有矩形區塊（含 x1,x2,y_mid,title）。
    2) 呼叫 `_collect_scc_calibration_by_lane()`：
       - 僅在 SCC1~SCC3 車道且 title 可匹配 _RE_SCC 的矩形取校準點；
       - 端點可見且寬度達門檻則用 (x1,開始)、(x2,結束)，否則用中點；
       - 逐車道進行跨日展開（最多 +1 天），再合併依 x 排序回傳 (xs, ts)。
    3) 使用 _piecewise_linear() 建立 x→time 對應，將 LF 車道的每個矩形
       (x1,x2) 轉為 (start,end)；若 end < start，補 +1 天。
    4) 以 now 將各筆記錄分到 past/current/future；current 的「製程狀態」標為「表定」。

    參數
    ----
    now : datetime | None
        用於分桶的基準時間；預設為目前時間。
    pool : urllib3.PoolManager | None
        HTTP 連線池；未提供時使用預設的 `_POOL`。

    回傳
    ----
    ScheduleResult：包含 ok、三個 DataFrame（past/current/future）與失敗原因字串。
    """
    now = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    pool = pool or _POOL

    areas = _fetch_2133_areas(pool)
    if not areas:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "連線逾時或頁面無資料")

    fixed_scc = {k: v for k, v in _FIXED_LANES.items() if k.startswith("SCC")} or None
    xs, ts = _collect_scc_calibration_by_lane(areas, now, fixed_scc_lanes=fixed_scc)

    if len(xs) < 2:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "SCC 標定點不足，無法校準")

    # 掃描所有矩形，找屬於 LF 的灰/紅矩形，將 x1→start、x2→end
    recs = []
    for r in areas:
        tag = _lane_of(r["y_mid"], fixed_lanes = _FIXED_LANES)
        if not tag:
            continue
        # 爐號
        m = re.search(r"爐號[＝>:\s]*([A-Za-z0-9]+)", r["title"])
        furnace = m.group(1) if m else "未知"

        # x→time（用分段線性插值；先把查詢點插到 xs/ts上）
        start = _piecewise_linear(r["x1"], xs, ts)
        end   = _piecewise_linear(r["x2"], xs, ts)
        if end < start:
            end += pd.Timedelta(days=1)

        recs.append((r["x1"], start, end, furnace, tag))

    # 排序/去重/跨日修正（重用你既有工具）
    recs = _sort_schedules(recs)
    recs = _deduplicate(recs)
    recs = _adjust_cross_day(recs, now)

    # 分桶
    past, current, future = [], [], []
    for x, start, end, furnace, proc in recs:
        if end < now:
            past.append([start,end,furnace,proc])
        elif start > now:
            future.append([start,end,furnace,proc])
        else:
            current.append([start,end,furnace,proc,"表定"])

    return ScheduleResult(
        ok=True,
        past=pd.DataFrame(past, columns=["開始時間","結束時間","爐號","製程"]),
        current=pd.DataFrame(current, columns=["開始時間","結束時間","爐號","製程","製程狀態"]),
        future=pd.DataFrame(future, columns=["開始時間","結束時間","爐號","製程"]),
        reason="",
    )

def scrape_schedule_2133_actual(*, now: Optional[datetime] = None, pool: Optional[urllib3.PoolManager] = None) -> ScheduleResult:
    """
    2133「實際」：以 SCC 校準建立 x→time 映射；對於位於 LF1/LF2 車道且 title 含 STEPn 的矩形，
    以其 (x1,x2) 投影為『實際開始/結束時間』。不再讀取 title 內 STEPn 的時間字串。
    """
    now_ts = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    pool = pool or _POOL

    # 讀 2133
    areas = _fetch_2133_areas(pool)
    if not areas:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "2133 連線失敗或頁面無資料")

    # 1) 用 SCC1~SCC3 收集校準點，建立 x→time 映射（與 planned 相同）
    fixed_scc = {k: v for k, v in _FIXED_LANES.items() if str(k).upper().startswith("SCC")}
    xs, ts = _collect_scc_calibration_by_lane(areas, now_ts, fixed_scc_lanes=fixed_scc)
    if len(xs) < 2:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "SCC 標定點不足（實際）")

    # 2) 篩 LF 矩形：必須 (a) 落在 LF 固定範圍、(b) title 含 STEP
    fixed_lf = {k: v for k, v in _FIXED_LANES.items() if str(k).upper().startswith("LF")}
    recs = []
    for r in areas:
        title = r.get("title", "") or ""
        if "STEP" not in title.upper():
            continue
        lane = _lane_of(r["y_mid"], fixed_lanes=fixed_lf)  # 僅用固定範圍
        if lane not in ("LF1", "LF2"):
            continue

        # 爐號（可無則設未知）
        m = re.search(r"爐號[＝>:\s]*([A-Za-z0-9\-]+)", title)
        furnace = m.group(1) if m else "未知"

        # 3) 用校準 xs/ts 映射 x1/x2 → 實際 start/end，不讀取 STEPn 時間
        start = _piecewise_linear(r["x1"], xs, ts)
        end   = _piecewise_linear(r["x2"], xs, ts)
        if end < start:
            end += pd.Timedelta(days=1)

        recs.append((start, end, furnace, lane, "實際"))

    # 無資料
    if not recs:
        return ScheduleResult(True, _empty_df(), _empty_df(), _empty_df(), "")

    # 4) 組表＆分桶
    df = pd.DataFrame(recs, columns=["開始時間","結束時間","爐號","製程","製程狀態"]).sort_values(["開始時間","結束時間","爐號","製程"], ignore_index=True)
    past_df   = df[df["結束時間"] <  now_ts][["開始時間","結束時間","爐號","製程"]].reset_index(drop=True)
    current_df= df[(df["開始時間"] <= now_ts) & (df["結束時間"] >= now_ts)].reset_index(drop=True)
    future_df = df[df["開始時間"] >  now_ts][["開始時間","結束時間","爐號","製程"]].reset_index(drop=True)
    # current 需包含製程狀態欄
    if not current_df.empty and "製程狀態" not in current_df.columns:
        current_df["製程狀態"] = "實際"
    else:
        current_df = current_df[["開始時間","結束時間","爐號","製程","製程狀態"]]

    return ScheduleResult(True, past_df, current_df, future_df, "")

# === 追加：2143（LF1/LF2 即時狀態） =====================================
def scrape_lf_status_2143(pool: Optional[urllib3.PoolManager]=None) -> dict:
    """
    解析 2143 頁面，擷取：
    LF1: 爐號(lbllf1_heat)、開始(lblLf1_Stime)、結束(lbllf1_Etime)、狀態(lblLF1sts)、停機(lblLF1Stime)
    LF2: 爐號(lbllf2_heat)、開始(lblLf2_Stime)、結束(lbllf2_Etime)、狀態(lblLF2sts)、停機(lblLF2Stime)
    """
    pool = pool or _POOL
    soup = _fetch_soup(URL_2143, pool)
    if soup is None:
        return {"ok": False, "reason": "連線逾時或頁面無資料"}

    def get(id_):
        sp = soup.find("span", {"id": id_})
        return sp.text.strip() if sp else ""

    data = {
        "ok": True,
        "LF1": {
            "爐號": get("lbllf1_heat"),
            "開始處理時間": get("lblLf1_Stime"),
            "處理結束時間": get("lbllf1_Etime"),
            "生產狀態": get("lblLF1sts"),
            "停機時間": get("lblLF1Stime"),
        },
        "LF2": {
            "爐號": get("lbllf2_heat"),
            "開始處理時間": get("lblLf2_Stime"),
            "處理結束時間": get("lbllf2_Etime"),
            "生產狀態": get("lblLF2sts"),
            "停機時間": get("lblLF2Stime"),
        },
    }
    return data

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
def _fetch_2133_areas(pool: urllib3.PoolManager) -> List[dict]:
    """
    擷取 2133 頁面所有 <area> 並標準化為 dict。

    回傳的每筆 dict 欄位
    --------------------
    - "x1","y1","x2","y2" : int   影像座標
    - "y_mid"             : int   (y1+y2)//2，車道判斷用
    - "title"             : str   原始 title 文字（含爐號/步驟/時間資訊）

    說明
    ----
    - 若連線失敗或頁面無資料，回傳空清單。
    - 非法座標（coords <4 個）會略過，避免影響流程。
    """
    soup = _fetch_soup(URL_2133, pool)
    if soup is None:
        return []
    out = []
    for a in soup.find_all("area"):
        title = a.get("title", "") or ""
        coords = [int(x) for x in re.findall(r"\d+", a.get("coords",""))]
        if len(coords) < 4:
            continue
        x1,y1,x2,y2 = coords[:4]
        y_mid = (y1 + y2)//2
        out.append({"x1":x1,"y1":y1,"x2":x2,"y2":y2,"y_mid":y_mid,"title":title})
    return out

def _piecewise_linear(xq: float, xs: List[int], ts: List[pd.Timestamp]) -> pd.Timestamp:
    """
    以分段線性方式將座標 x 映射到時間 t（含左右外插與防呆）。

    規則
    ----
    - xs 與 ts 長度相同、已依 xs 遞增。
    - 左右外插：若 xq 在最左/最右之外，沿著第一/最後一段斜率外推；
      若端點多個 x 相同，會向內尋找最近一個不相等的節點。
    - 內插：在 [xs[j],xs[j+1]] 內線性插值；使用 bisect_right(xs, int(round(xq))) 尋段；
      如 `dx==0`（同 x），回傳左端時間以避免除零。

    參數
    ----
    xq : float
        欲映射的 x 座標。
    xs : List[int]
        校準節點的 x（遞增）。
    ts : List[pd.Timestamp]
        校準節點的時間（建議已過跨日展開）。

    回傳
    ----
    pd.Timestamp：對應時間。
    """
    n = len(xs)
    if n == 0:
        return pd.Timestamp.now()
    if n == 1:
        return ts[0]

    # 左端外插
    if xq <= xs[0]:
        i = 1
        while i < n and xs[i] == xs[0]:
            i += 1
        if i == n:
            return ts[0]
        dt = ts[i] - ts[0]
        dx = xs[i] - xs[0]
        if dx == 0:
            return ts[0]
        return ts[0] + (xq - xs[0]) * dt / dx

    # 右端外插
    if xq >= xs[-1]:
        i = n - 2
        while i >= 0 and xs[i] == xs[-1]:
            i -= 1
        if i < 0:
            return ts[-1]
        dt = ts[-1] - ts[i]
        dx = xs[-1] - xs[i]
        if dx == 0:
            return ts[-1]
        return ts[-1] + (xq - xs[-1]) * dt / dx

    # 內插
    import bisect
    j = bisect.bisect_right(xs, int(round(xq))) - 1
    x0, x1 = xs[j], xs[j+1]
    t0, t1 = ts[j], ts[j+1]
    dx = x1 - x0
    if dx == 0:
        return t0
    w = (xq - x0) / dx
    return t0 + (t1 - t0) * w

def _lane_of(y_mid: int, fixed_lanes: Optional[dict] = None) -> Optional[str]:
    """
    以固定 Y 範圍判斷 LF 車道。

    參數
    ----
    y_mid : int
        矩形 y 中心值。
    fixed_lanes : dict | None
        固定範圍，如 {"LF1":{"min":a,"max":b}, "LF2":{"min":c,"max":d}}。
        本函式僅在提供固定範圍時回傳 'LF1' 或 'LF2'，否則回傳 None。

    回傳
    ----
    'LF1' | 'LF2' | None
    """
    lf1 = fixed_lanes.get("LF1", {})
    lf2 = fixed_lanes.get("LF2", {})
    if lf1 and (lf1.get("min") is not None) and (lf1.get("max") is not None):
        if lf1["min"] <= y_mid <= lf1["max"]:
            return "LF1"
    if lf2 and (lf2.get("min") is not None) and (lf2.get("max") is not None):
        if lf2["min"] <= y_mid <= lf2["max"]:
            return "LF2"
    return None

def _collect_scc_calibration_by_lane(
    areas: List[dict],
    now: pd.Timestamp,
    fixed_scc_lanes: Optional[Dict[str, Dict[str, float]]] = None,
) -> Tuple[List[int], List[pd.Timestamp]]:
    """
    僅在 SCC1~SCC3 車道收集校準點（title 必須包含 SCC 的開始/結束時間），
    優先取可見端點（寬度達門檻），否則取中點；逐車道做跨日展開後合併並依 x 排序。

    規則
    ----
    - 車道判斷：僅採用 `fixed_scc_lanes`（固定 Y 範圍），非該車道一律略過。
    - 候選矩形需通過 _RE_SCC 正則（擷取開始/結束時間）。
    - 端點策略：以候選之 min(x1)/max(x2) 近似可視區，寬度 < MIN_W 或端點不在可視區則改用中點。
    - 逐車道以 _fix_cross_day_sequence() 展開後，再把三車道合併，依 x 排序後回傳 (xs, ts)。

    參數
    ----
    areas : List[dict]
        2133 頁面解析出的矩形（含 x1,x2,y_mid,title）。
    now : pd.Timestamp
        展開/歸屬的時間基準。
    fixed_scc_lanes : Optional[Dict[str, Dict[str, float]]]
        固定的 SCC1/2/3 Y 範圍；若未提供則不收集（本版本僅用固定範圍）。

    回傳
    ----
    (xs, ts) ：校準點的 x 與時間，皆已依 x 遞增。
    """
    PAD = 2
    MIN_W = 8  # 過窄的端點退回中點

    def rect_visible_ok(r: dict, view_left: int, view_right: int) -> bool:
        x1, x2 = int(r["x1"]), int(r["x2"])
        if (x2 - x1) < MIN_W:
            return False
        return (x1 >= view_left + PAD) and (x2 <= view_right - PAD)

    def compute_view_bounds(cands: List[dict]) -> Tuple[int, int]:
        """暫以本批候選的 min(x1)/max(x2) 近似可視區；若你有真實 view 可直接替換。"""
        if not cands:
            return 0, 10**9
        left = min(int(r["x1"]) for r in cands)
        right = max(int(r["x2"]) for r in cands)
        return left, right

    # 透過fixed lance 判斷是屬於SCC1~3
    def lane_by_y(y: float) -> Optional[str]:
        for name, rng in fixed_scc_lanes.items():
            y_min = rng.get("min")
            y_max = rng.get("max")
            if y_min <= y <= y_max:
                return name
        return None

    # 只取 在SCC1~3 Y通道，且title 有 SCC 開始/結束的矩形
    candidates = [
        r for r in areas
        if _RE_SCC.search(r.get("title", ""))
           and (lane_by_y(float(r["y_mid"])) in {"SCC1", "SCC2", "SCC3"})
    ]

    view_left, view_right = compute_view_bounds(candidates)

    by_lane: Dict[str, List[Tuple[int, pd.Timestamp]]] = {"SCC1": [], "SCC2": [], "SCC3": []}
    today = now.normalize().date().isoformat()

    for r in candidates:
        lane = lane_by_y(float(r["y_mid"]))
        if lane is None or lane not in by_lane:
            continue

        m = _RE_SCC.search(r.get("title", ""))
        if not m:
            continue
        s, e = m.groups()
        t0 = pd.to_datetime(f"{today} {s}")
        t1 = pd.to_datetime(f"{today} {e}")
        if t1 < t0:  # 同一矩形內跨午夜
            t1 += pd.Timedelta(days=1)

        x1, x2 = int(r["x1"]), int(r["x2"])
        if rect_visible_ok(r, view_left, view_right):
            # 端點兩個校準點
            by_lane[lane].append((x1, pd.Timestamp(t0)))
            by_lane[lane].append((x2, pd.Timestamp(t1)))
        else:
            # 中點一個校準點
            x_mid = (x1 + x2) // 2
            t_mid = t0 + (t1 - t0) / 2
            by_lane[lane].append((x_mid, pd.Timestamp(t_mid)))

    # 逐車道展開，再合併
    xs_all: List[int] = []
    ts_all: List[pd.Timestamp] = []

    for lane, pts in by_lane.items():
        if not pts:
            continue
        pts.sort(key=lambda t: t[0])  # 依 x 排序
        xs_lane = [p[0] for p in pts]
        ts_lane = [p[1] for p in pts]

        # 比照 _adjust_cross_day 規則的版本
        ts_lane = _fix_cross_day_sequence(
            ts_lane,
            epsilon_minutes=1.0,
        )
        xs_all.extend(xs_lane)
        ts_all.extend(ts_lane)

    # 合併後依 x 全域排序
    order = sorted(range(len(xs_all)), key=lambda i: xs_all[i])
    xs_all = [xs_all[i] for i in order]
    ts_all = [ts_all[i] for i in order]
    return xs_all, ts_all

def _fix_cross_day_sequence(
    ts_list: Sequence[pd.Timestamp],
    *,
    epsilon_minutes: float = 1.0,
) -> List[pd.Timestamp]:
    """
    針對「同一車道、按 x 由左到右」的時間序列做跨日展開修正。

    規則
    ----
    - 先依「清晨視窗」與「第一點距 now 的差」決定是否將整列平移 ±1 天，避免全列落在隔日。
    - 再從左到右檢查相鄰差值，若偵測時間倒退（小於 -epsilon），對該點加上一天。

    參數
    ----
    ts_list : Sequence[pd.Timestamp]
        已依 x 排序的同車道時間序列。
    epsilon_minutes : float
        容忍值（分鐘）；小幅抖動不視為倒退。

    回傳
    ----
    List[pd.Timestamp]：展開後的新序列（不就地修改原輸入）。
    """
    if not ts_list:
        return []

    out: List[pd.Timestamp] = [pd.Timestamp(t) for t in ts_list]
    eps  = pd.Timedelta(minutes=epsilon_minutes)
    now = pd.Timestamp.now()

    # --- 1) 依「清晨視窗」與「第一點距 now 的距離」做全序列平移（對齊 _adjust_cross_day） ---
    # 清晨：若與 now 差超過 10 小時，整體 -1 天
    if now.time() < pd.Timestamp("08:00").time():
        if abs(now - out[0]) > pd.Timedelta(hours=10):
            out = [t - pd.Timedelta(days=1) for t in out]
    # 非清晨：若第一點與 now 差超過 10 小時，整體 +1 天
    elif abs(now - out[0]) > pd.Timedelta(hours=10):
        out = [t + pd.Timedelta(days=1) for t in out]

    prev: Optional[pd.Timestamp] = None
    for i, t in enumerate(out):
        if prev is not None:
            delta = t - prev
            if delta < -eps:
                t = t + pd.Timedelta(days=1)
        out[i] = t
        prev = t
    return out

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

def _empty_df() -> pd.DataFrame:
    """回傳具有預期欄位的空白 DataFrame。

    回傳的欄位應與正常解析後的 DataFrame 一致，以降低 UI 或後續處理出現
    KeyError 的風險。若暫時未定義欄位，也可先回傳完全空表，日後再補齊。

    Returns:
        pd.DataFrame: 空的 DataFrame。建議包含預期欄位
            （例如：["開始時間", "結束時間", "製程", "爐號", "製程狀態"]）。
    """
    return pd.DataFrame()

if __name__ == "__main__":  # pragma: no cover
    p_df, c_df, f_df, status = scrape_schedule()
    print("Past\n", p_df.tail())
    print("Current\n", c_df)
    print("Future\n", f_df.head())
"""schedule_scraper.py

Scrapes Dragon Steel MES “2138” (schedule chart) and “2137” (status page),
and “2133/2143” (LF) then classifies each record into **past**, **current** or
**future** relative to *now*.

Key points
----------
- **2138 duplicate-in-same-process fix**:
  When the same furnace re-enters the same process and a single rectangle title
  contains multiple time spans (e.g. two HH:MM~HH:MM pairs), we now **pair the
  X-coordinate with times by position inside each (furnace, process, label) group**.
  Concretely, we rank records twice per group—once by start-time and once by X—
  and merge on the positional index so that:
      smallest X ↔ earliest time, next X ↔ next time, ...
  This preserves the intended positive correlation between X (left→right) and time.

- 2137/2143 provide channel-wise “current” status (furnace id and a time window)
  to split 2138/2133 rectangles into past / current / future.

- The UI no longer owns heavy parsing logic; this helper returns data frames
  ready for rendering.

Returned data frames (past/current/future) include at least:
* 表定開始時間 / 表定結束時間
* 實際開始時間 / 實際結束時間
* 爐號, 製程, phase（分類結果）
* current 另帶製程狀態（便於著色/提示）
"""

from __future__ import annotations
from bs4 import BeautifulSoup
import re, urllib3
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Set, Sequence, Any
import pandas as pd
import numpy as np
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
_FIXED_LANES_2138 = {
    "EAFA": {"min":179, "max": 197},
    "EAFB": {"min":217, "max": 235},
    "LF1-1": {"min":250, "max": 268},
    "LF1-2": {"min":286, "max":304},
}

# Y‑axis pixel ranges of the bitmap‐map that identify each process row on 2133
_FIXED_LANES_2133 = {
    "LF1": {"min": 281, "max": 309},
    "LF2": {"min": 322, "max": 348},
    "SCC1": {"min": 487, "max": 513},
    "SCC2": {"min": 528, "max": 554},
    "SCC3": {"min": 569, "max": 596},
}

# === 高度→類別規則（可依實際觀察再微調） ============================
_HEIGHT_RULES: Dict[str, Dict[str, Dict[str, Any]]] = {
    # 2138 電爐場
    "2138": {
        "planned":          {"heights": {11},       "tol": 0, "label": "表定"},
        "actual":           {"heights": {7, 8},     "tol": 0, "label": "實際"},
        "actual_corrected": {"heights": {4, 5},     "tol": 0, "label": "校正實際", "only_in": {"EAFA","EAFB"}},
    },
    # 2133 轉爐場
    "2133": {
        "planned":          {"heights": {16, 17},   "tol": 0, "label": "表定"},
        "actual":           {"heights": {8, 9},     "tol": 0, "label": "實際"},
        # 2133 未觀察到綠色（校正實際）
    },
}

# Title patterns used by MES page when hovering the area map.
_TIME_PATTERNS: Dict[str, str] = {
    proc: rf"{proc}時間:\s*(\d{{2}}:\d{{2}}:\d{{2}})\s*~\s*(\d{{2}}:\d{{2}}:\d{{2}})"  # noqa: E501
    for proc in _FIXED_LANES_2138
}

# 2133：title 辨識
_RE_SCC = re.compile(r"SCC開始時間\s*:\s*(\d{2}:\d{2}:\d{2}).*?SCC結束時間\s*:\s*(\d{2}:\d{2}:\d{2})", re.S)
# 2138：把某些 title 判為「輔助層」
_AUX_TITLE_PAT = re.compile(r"(送電)", re.I)

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

@dataclass
class RectClassify:
    page: str                    # '2138' or '2133'
    lane: Optional[str]          # 推定通道（例如 'EAFA','EAFB','LF1','LF2'），若無固定範圍則可能為 None
    kind: str                    # 'planned'|'actual'|'actual_corrected'|'aux'|'unknown'
    label: str                   # '表定'|'實際'|'校正實際'|'輔助'|'未知'
    confidence: float            # 0~1 簡單信心分數
    reason: str                  # 判斷依據說明（除錯用）

# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------
def scrape_schedule(
        *,
        now: datetime = None,
) -> ScheduleResult:
    """
    以「你定義的新規則」整合 2138/2137 與 2133/2143 的資料來源，產出 past/current/future 三分表。

    資料來源與角色
    --------------
    - 2138（電爐場圖）：以 <area> 解析為「表定/實際/輔助」三類：
        * 類別依長條高度 (h=|y2-y1|) 判斷：
            - 表定：h≈11
            - 實際：h≈7~8
            - 輔助（送電刻度、只見於 EAFA/EAFB）：h≈4~5，僅供對齊，不直接輸出
        * 通道以固定 Y 範圍判斷（EAFA/EAFB/LF1-1/LF1-2）
        * 時間以 title 的「{製程}時間: HH:MM:SS ~ HH:MM:SS」解析
    - 2137（電爐場狀態頁）：提供每通道目前「爐號」與「開始/結束」時間（分鐘精度），
        用來把 2138 的長條分出 past/current/future。

    - 2133（轉爐場圖）：LF1/LF2 的表定/實際長條（高度規則不同）：
        * 表定：h≈16~17
        * 實際：h≈8~9
        * 通道以固定 Y 範圍判斷（LF1/LF2）
        * 開始/結束時間改用「SCC1~3 的校準點」做 x→time 分段線性映射得到（不再讀 STEPn 時間）
    - 2143（LF 即時頁）：提供 LF1/LF2 當前「爐號」與「開始/結束/停機」，
        用來把 2133 的長條分出 past/current/future。

    分類規則（phase）
    -----------------
    * 2138 × 2137：
        - past：該筆有「實際開始與實際結束」，或（其他情況下）已完全早於 now
        - future：表定開始/結束皆在 now 之後，且（若 2137 有同通道爐號）表定開始與狀態開始差 > 30 分鐘
        - current：與 2137 同通道爐號匹配，且表定開始與狀態開始差 < 30 分鐘
    * 2133 × 2143：
        - past：該筆有「實際開始與實際結束」且（2143 同爐時）狀態結束存在
        - future：表定開始/結束皆在 now 之後，且（若 2143 有同通道爐號）表定開始與狀態開始差 > 30 分鐘
        - current：與 2143 同通道爐號匹配，且表定開始與狀態開始差 < 30 分鐘

    2138 的多時段同爐修正
    --------------------
    同一爐號在同一製程（含相同 label，例如「表定」或「實際」）若於單一 title 中出現
    多組時間（HH:MM~HH:MM），會先把每筆解析成 (x, start, end, 爐號, 製程, label)。
    接著於「(爐號, 製程, label)」各組內：
      1) 依 start 升冪標上位置索引 _pos
      2) 依 x 升冪標上位置索引 _pos
      3) 以 (keys + _pos) merge，得到「最小 x ↔ 最早 start」的一一對應
    配對後的正確 (x, start, end) 會回填進後續流程，避免 phase 判斷混亂。

    回傳欄位
    --------
    ScheduleResult（past/current/future 皆為 DataFrame）：
      - 「開始時間」「結束時間」預設用表定（current/future），past 以實際為主；
        你程式內同時保留了「表定開始時間/表定結束時間」「實際開始時間/實際結束時間」，
        供上游 hover/對齊與後續運算使用。
      - 「製程狀態」僅在 current 中填入（便於 UI 著色/提示）。

    失敗處理
    --------
    - 任一關鍵頁面取回失敗（逾時/非 200）→ 回傳 ScheduleResult(ok=False, 三表皆空, reason)
    - 校準點不足（2133 的 SCC 校準 < 2）→ 回傳 ok=False 並說明

    參數
    ----
    now : datetime | None
        分類的基準時間。預設為 `pd.Timestamp.now()`。

    備註
    ----
    - 此函式只包裝資料整合與分類，不拋出例外到 UI；請以回傳之 ok/reason 判斷。
    - 高度判斷與固定 Y 範圍可依 MES 版面變動調整（集中在 _HEIGHT_RULES 與 _FIXED_LANES_*）。
    """
    if now is None:
        now = pd.Timestamp.now()

    # ------------------------------------------------------------------
    # 1. Schedule rectangles from 2138 ---------------------------------
    # ------------------------------------------------------------------
    soup_2138 = _fetch_soup(URL_2138, _POOL)
    soup_2137 = _fetch_soup(URL_2137, _POOL)

    if soup_2138 is None or soup_2137 is None:
        return ScheduleResult(
            ok=False,
            past=_empty_df(),
            current=_empty_df(),
            future=_empty_df(),
            reason="連線逾時或伺服器暫時無回應",
        )

    areas = soup_2138.find_all("area")
    raw_sched: List[Tuple[int, datetime, datetime, str, str, str]] = []
    fixed_2138 = _FIXED_LANES_2138
    multi_proc = []  # 儲存發生相同爐號重覆進同一個製程時的記錄，並用來判斷是否做後續動作。

    for area in areas:
        title = area.get("title", "")
        coords = [int(x) for x in re.findall(r"\d+", area.get("coords", ""))]

        if len(coords) < 4:
            continue
        x1, y1, x2, y2 = coords
        y_mid = (y1+y2)/2
        process_type = _lane_by_y(y_mid, fixed_2138)
        if process_type is None or process_type not in title:
            continue

        res = _classify_rectangle("2138", coords, title, fixed_2138)

        furnace_match = re.search(r"爐號[＝>:\s]*([A-Za-z0-9]+)", title)
        furnace_id = furnace_match.group(1) if furnace_match else "未知"

        # The times in the green rectangles don't include seconds, so we have to handle them separately.
        """
        debug: 相同爐號重覆進同一個製程 (EAF、LF1-1、LF1-2)，在_preprocess_schedule() 後會發生製程錯誤
        re 在匹配時，改用findall 以list 的方式，回傳所有匹配的資料
        """
        if res.label == "輔助":
            #m = re.search(rf"{process_type}送電:\s*(\d{{2}}:\d{{2}})\s*~\s*(\d{{2}}:\d{{2}})", title)
            m = re.findall(rf"{process_type}送電:\s*(\d{{2}}:\d{{2}})\s*~\s*(\d{{2}}:\d{{2}})", title)
        else:
            #m = re.search(_TIME_PATTERNS[process_type], title)
            m = re.findall(_TIME_PATTERNS[process_type], title)

        today = now.date().isoformat()
        if not m:
            continue
        """ 
            如果匹配出來的時間<2, 代表同一爐號沒有重覆進同一製程的問題
            >=，也存在另一個list，待後續匹配正確的x座標和開始時間。
        """
        if len(m) < 2:
           start_ts, end_ts = m[0]
           start = pd.to_datetime(f"{today} {start_ts}")
           end = pd.to_datetime(f"{today} {end_ts}")
           raw_sched.append((coords[0], start, end, furnace_id, process_type, res.label))
        else:
            for i in range(len(m)):
                start_ts, end_ts = m[i]
                start = pd.to_datetime(f"{today} {start_ts}")
                end = pd.to_datetime(f"{today} {end_ts}")
                multi_proc.append((coords[0], start, end, furnace_id, process_type, res.label))

    if multi_proc:
        # 同爐號在同製程同 label 的「多時間段」情境：
        # 以 (爐號, 製程, 類別) 為分組鍵，對時間與 x 各自排序並標上 cumcount() 位置，
        # 再以 (keys + _pos) merge 取得「最小 x ↔ 最早時間」的正相關一一配對，
        # 之後將修正過的清單 append 回 raw_sched，避免後續 _preprocess_schedule() 出現錯位。
        multi_proc_df = pd.DataFrame(multi_proc)

        # keys: 你要在同一群組內配對；這裡用(爐號，製程，種類)
        keys = [3,4,5]
        # 1)依時間排序並標上組內位置
        left = (multi_proc_df.sort_values(keys + [1])
                .assign(_pos = lambda d: d.groupby(keys).cumcount()))
        # 2) 依座標排序並標上組內位置 (只保留座標欄)
        right = (multi_proc_df.sort_values(keys + [0])  # 0 是「座標」
        .assign(_pos=lambda d: d.groupby(keys).cumcount())
        [keys + ['_pos', 0]])

        # 3) 用 (keys + 位置) 對齊，得到正確配對後的座標
        correct_df = (left.drop(columns=[0])  # 先把原本的 0 刪掉
               .merge(right, on=keys + ['_pos'], how='left')
               .drop(columns=['_pos']))
        correct_df = correct_df[[0,1,2,3,4,5]]
        correct_list = list(correct_df.itertuples(index=False, name=None))
        raw_sched = raw_sched + correct_list

    # If no schedule is found after parsing the webpage, initialize schedule_2133 as an
    # empty DataFrame with predefined columns.
    if raw_sched:
        # Sort, adjust cross day, and merge schedule
        schedule_2138 = _preprocess_schedule(raw_sched)
    else:
        schedule_2138 = pd.DataFrame(columns=['製程', '爐號','表定開始時間',
                                              '表定結束時間','實際開始時間','實際結束時間'])

    # ------------------------------------------------------------------
    # 2. Process status from 2137 and merge with 2138 ------------------
    # ------------------------------------------------------------------
    labels_2137 = _scrape_2137_labels(pool=_POOL, now=now)  # 你新增的 2137 抓取函式；或先用硬編輯測試
    status_2137_df = pd.DataFrame(labels_2137)
    status_2137 = (status_2137_df
    .T
    .reset_index()
    .rename(columns={
        'index': '製程',
        '爐號': '狀態爐號',
        'start': '狀態開始',
        'finish': '狀態結束',
        'status': '狀態'
    })
    )

    status_2137['狀態開始'] = pd.to_datetime(status_2137['狀態開始'])
    status_2137['狀態結束'] = pd.to_datetime(status_2137['狀態結束'])
    s_2138_classify = schedule_2138.merge(status_2137, left_on=['製程', '爐號'], right_on=['製程', '狀態爐號'], how='left')

    a_s = pd.to_datetime(s_2138_classify['實際開始時間'])
    a_e = pd.to_datetime(s_2138_classify['實際結束時間'])
    p_s = pd.to_datetime(s_2138_classify['表定開始時間'])
    p_e = pd.to_datetime(s_2138_classify['表定結束時間'])

    # A schedule is classified as "past" if both columns ('actual start time' and 'actual end time')
    # are notna().
    mask_1 = a_s.notna() & a_e.notna()

    # A schedule is classified as "future" if all the following conditions are met:
    # (1) Both columns('plan start time' and 'plan end time') are greater than current time.
    # (2) 'Status furnace id' matches 'furnace id', and the time difference between 'plan start'
    #     and 'status start' is greater than 30 minutes. This prevents a subsequent scheduled
    #     entry for the same furnace (e.g. a second run) from being incorrectly treated as the
    #     current run.

    s_furnace_id = s_2138_classify['狀態爐號']
    s_s = s_2138_classify["狀態開始"]
    diff = ~(p_s.sub(s_s) < pd.Timedelta(minutes=30))

    mask_2 = (p_s.gt(now)
              & p_e.gt(now)
              & (s_furnace_id.isna()
                 | (~s_furnace_id.isna()
                    & diff
                    )
            )
    )
    # A schedule is classified as "current" if 'Status furnace id' matches 'furnace id',
    # and the time difference between 'plan start' and 'status start' is less than 30 minutes.
    mask_3 = ~diff

    s_2138_classify['phase'] = np.select(
        [mask_1, mask_2, mask_3],
        ['past', 'future', 'current'],
        default='unknown'
    )

    # ------------------------------------------------------------------
    # 3. Schedule rectangles from 2133 ---------------------------------
    # ------------------------------------------------------------------
    areas_2133 = _fetch_2133_areas(_POOL)
    soup_2133 = _fetch_soup(URL_2133, _POOL)
    soup_2143 = _fetch_soup(URL_2143, _POOL)
    raw_sched: List[Tuple[int, datetime, datetime, str, str, str]] = []
    fixed_2133 = _FIXED_LANES_2133

    if soup_2133 is None or soup_2143 is None:
        return ScheduleResult(
            ok=False,
            past=_empty_df(),
            current=_empty_df(),
            future=_empty_df(),
            reason="連線逾時或伺服器暫時無回應",
        )
    a_2133 = _fetch_2133_areas(_POOL)
    if not areas_2133:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "連線逾時或頁面無資料")

    fixed_scc = {k: v for k, v in fixed_2133.items() if k.startswith("SCC")} or None
    xs, ts = _collect_scc_calibration_by_lane(a_2133, now, fixed_scc_lanes=fixed_scc)

    if len(xs) < 2:
        return ScheduleResult(False, _empty_df(), _empty_df(), _empty_df(), "SCC 標定點不足，無法校準")

    # 掃描所有矩形，找屬於 LF 的灰/紅矩形，將 x1→start、x2→end

    areas_2133 = soup_2133.find_all("area")
    for area in areas_2133:
        title = area.get("title", "")
        coords = [int(x) for x in re.findall(r"\d+", area.get("coords", ""))]
        if len(coords) < 4:
            continue
        x1, y1, x2, y2 = coords
        y_mid = (y1+y2)/2

        process_type = _lane_by_y(y_mid, fixed_2133)
        if process_type is None:
            continue
        if process_type not in ("LF1", "LF2"):
            continue

        res = _classify_rectangle("2133", coords, title, fixed_2133)
        furnace_match = re.search(r"爐號[＝>:\s]*([A-Za-z0-9]+)", title)
        furnace_id = furnace_match.group(1) if furnace_match else "未知"

        # x→time（用分段線性插值；先把查詢點插到 xs/ts上）
        start = _piecewise_linear(coords[0], xs, ts)
        end   = _piecewise_linear(coords[2], xs, ts)

        # 跨天檢查
        if end < start:
            end += pd.Timedelta(days=1)

        # 去掉時間過短的紅色rectangle
        if (end - start) < pd.Timedelta(minutes=5) and res.label == '實際':
            continue

        raw_sched.append((coords[0], start, end, furnace_id, process_type, res.label))

    # If no schedule is found after parsing the webpage, initialize schedule_2133 as an
    # empty DataFrame with predefined columns.
    if raw_sched:
        # Sort, adjust cross day, and merge schedule
        schedule_2133 = _preprocess_schedule(raw_sched)
    else:
        schedule_2133 = pd.DataFrame(columns=['爐號','製程','表定開始時間',
                                              '表定結束時間','實際開始時間','實際結束時間'])

    # ------------------------------------------------------------------
    # 4. Process status from 2143 and merge with 2133 ------------------
    # ------------------------------------------------------------------
    labels_2143 = _scrape_lf_status_2143(pool=_POOL, now=now)  # 你新增的 2137 抓取函式；或先用硬編輯測試
    status_2143 = (pd.DataFrame(labels_2143)
    .T
    .reset_index()
    .rename(columns={
        'index': '製程',
        '爐號': '狀態爐號',
        '開始處理時間': '狀態開始',
        '處理結束時間': '狀態結束',
        '生產狀態': '狀態'
        })
    )
    status_2143['狀態開始'] = pd.to_datetime(status_2143['狀態開始'])
    status_2143['狀態結束'] = pd.to_datetime(status_2143['狀態結束'])
    s_2133_classify = schedule_2133.merge(status_2143, left_on=['製程', '爐號'], right_on=['製程', '狀態爐號'], how='left')

    a_s = pd.to_datetime(s_2133_classify['實際開始時間'])
    a_e = pd.to_datetime(s_2133_classify['實際結束時間'])
    p_s = pd.to_datetime(s_2133_classify['表定開始時間'])
    p_e = pd.to_datetime(s_2133_classify['表定結束時間'])

    # A schedule is classified as "past" if all the following conditions are met:
    # (1) Both columns 'actual start time' and 'actual end time' are present (not NaT).
    # (2) Either the furnace ID does not appear on page 2143,
    #     or --if it does -- the 'status end time' is present (not NaT)
    c_fid_met = (s_2133_classify['爐號'] == s_2133_classify['狀態爐號'])
    proc_finished = ~(s_2133_classify['狀態結束'].isna())
    mask_1 = (
            a_s.notna()
            & a_e.notna()
            & ( ~c_fid_met |
                ( c_fid_met
                 & proc_finished
                )
            )
    )

    # A schedule is classified as "future" if all the following conditions are met:
    # (1) Both columns('plan start time' and 'plan end time') are greater than current time.
    # (2) 'Status furnace id' does not match 'furnace id', and if so the time difference
    #     between 'plan start' and 'status start' is greater than 30 minutes. This prevents
    #     a subsequent scheduled entry for the same furnace (e.g. a second run) from being
    #     incorrectly treated as the current run.
    s_furnace_id = s_2133_classify['狀態爐號']
    s_s = s_2133_classify["狀態開始"]
    mask_2 = (p_s.gt(now)
              & p_e.gt(now)
              & (s_furnace_id.isna()
                 | (~s_furnace_id.isna()
                    & (p_s.sub(s_s) > pd.Timedelta(minutes=30)
                    )
                 )
              )
    )

    # A schedule is classified as "current" if 'Status furnace id' matches 'furnace id',
    # and the time difference between 'plan start' and 'status start' is less than 30 minutes.
    mask_3 = p_s.sub(s_s) < pd.Timedelta(minutes=30)

    s_2133_classify['phase'] = np.select(
        [mask_1, mask_2, mask_3],
        ['past', 'future', 'current'],
        default='unknown'
    )
    out_df = pd.concat([s_2138_classify, s_2133_classify], join='inner')

    # This is a temporary workaround until the downstream code is updated.
    out_df = out_df.assign(開始時間=out_df["表定開始時間"], 結束時間=out_df["表定結束時間"])

    past_df = out_df.loc[out_df['phase'].eq('past'), :]
    current_df = out_df.loc[out_df['phase'].eq('current'), :]
    future_df = out_df.loc[out_df['phase'].eq('future'), :]

    return ScheduleResult(
        ok=True,
        past=past_df,
        current=current_df,
        future=future_df,
        reason=""
    )

# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------
def _scrape_2137_labels(*, pool: Optional[urllib3.PoolManager] = None,
                       now: Optional[pd.Timestamp] = None) -> dict:
    """
    抓取 2137 狀態頁（電爐場），回傳各通道的「爐號 / 開始 / 結束 / 狀態」字典。

    解析內容
    --------
    - EAFA：爐號(lbl_eafa_no)、開始(hh,mm)、結束(hh,mm)、狀態(lbl_eafa_period)
    - EAFB：同上（*_eafb_*）
    - LF1-1：爐號(lbl_lf11_no)、開始/結束(hh,mm)、狀態(lbl_lf11_period)
    - LF1-2：同上（*_lf12_*）

    時間處理
    --------
    - hh:mm 轉為 now.date() 的 `Timestamp`（秒補 :00）
    - 若結束小於開始，視為跨日 +1 天（簡易調整）

    回傳
    ----
    dict:
      {
        "EAFA": {"爐號": str, "start": Timestamp|None, "finish": Timestamp|None, "status": str},
        "EAFB": {...}, "LF1-1": {...}, "LF1-2": {...}
       OA 時間 (ph_lblShowNow_header)
      }
    """
    pool = pool or _POOL
    soup = _fetch_soup(URL_2137, pool)
    if soup is None:
        return {}

    def _txt(i: str) -> str:
        el = soup.find("span", {"id": i})
        return (el.text or "").strip() if el else ""

    def _parse_time(hh: str, mm: str, now_date: pd.Timestamp):
        hh = str(hh or "").strip()
        mm = str(mm or "").strip()
        if not hh or not mm or not hh.isdigit() or not mm.isdigit():
            return None
        t = pd.to_datetime(f"{now_date.date().isoformat()} {int(hh):02d}:{int(mm):02d}:00")
        # 防止讀取到的"開始處理時間"為前一天，造成「開始時間」、「預計完成時間」的日期錯誤
        # 目前暫時用解析出來的時間，與現在時間的差距是否超過10小時間判斷，並處理。
        if abs(t-now) > pd.Timedelta(hours=10):
            t -= pd.Timedelta(days=1)
        return t

    eafa_s = _parse_time(_txt("lbl_eafa_eh"), _txt("lbl_eafa_em"), now)
    eafa_f = _parse_time(_txt("lbl_eafa_fh"), _txt("lbl_eafa_fm"), now)
    eafb_s = _parse_time(_txt("lbl_eafb_eh"), _txt("lbl_eafb_em"), now)
    eafb_f = _parse_time(_txt("lbl_eafb_fh"), _txt("lbl_eafb_fm"), now)
    lf11_s = _parse_time(_txt("lbl_lf11_sh"), _txt("lbl_lf11_sm"), now)
    lf11_f = _parse_time(_txt("lbl_lf11_fh"), _txt("lbl_lf11_fm"), now)
    lf12_s = _parse_time(_txt("lbl_lf12_sh"), _txt("lbl_lf12_sm"), now)
    lf12_f = _parse_time(_txt("lbl_lf12_fh"), _txt("lbl_lf12_fm"), now)

    def _simple_adjust_cross(a, b):
        if a and b:
            if a > b:
                return (b+ pd.Timedelta(days=1))
            else:
                return b
            #b += pd.Timedelta(days=1) if a > b else b
        #return b
    # 簡易的跨天判斷
    eafa_f = _simple_adjust_cross(eafa_s, eafa_f)
    eafb_f = _simple_adjust_cross(eafb_s, eafb_f)
    lf11_f = _simple_adjust_cross(lf11_s, lf11_f)
    lf12_f = _simple_adjust_cross(lf12_s, lf12_f)

    # 依你給的 id 對應，轉成統一鍵名 start_h/start_m/finish_h/finish_m
    data = {
        # EAFA: eh/em/fh/fm
        "EAFA": {
            "爐號":    _txt("lbl_eafa_no"),
            "start":  eafa_s,
            "finish": eafa_f,
            "status":_txt("lbl_eafa_period"),
        },
        # EAFB: eh/em/fh/fm
        "EAFB": {
            "爐號":    _txt("lbl_eafb_no"),
            "start":  eafb_s,
            "finish": eafb_f,
            "status": _txt("lbl_eafb_period"),
        },
        # LF1-1: sh/sm/fh/fm
        "LF1-1": {
            "爐號":    _txt("lbl_lf11_no"),
            "start":  lf11_s,
            "finish": lf11_f,
            "status": _txt("lbl_lf11_period"),
        },
        # LF1-2: sh/sm/fh/fm
        "LF1-2": {
            "爐號":    _txt("lbl_lf12_no"),
            "start":  lf12_s,
            "finish": lf12_f,
            "status": _txt("lbl_lf12_period"),
        },
    }
    return data


def _scrape_lf_status_2143(pool: Optional[urllib3.PoolManager]=None,
                           now: Optional[pd.Timestamp] = None
                           ) -> dict:
    """
    抓取 2143（LF 即時）頁面，回傳 LF1/LF2 的「爐號 / 開始 / 結束 / 狀態 / 停機時間」。

    解析欄位
    --------
    - LF1：
        爐號(lbllf1_heat)、
        開始(lblLf1_Stime)、
        結束(lbllf1_Etime)、
        狀態(lblLF1sts)、
        停機(lblLF1Stime)
        OA 時間 (ph_lblShowNow_header)
    - LF2：
        爐號(lbllf2_heat)、
        開始(lbllf2_Stime)、
        結束(lbllf2_Etime)、
        狀態(lblLF2sts)、
        停機(lblLF2Stime)

    時間處理
    --------
    - 僅 hh:mm；以 now.normalize() 補齊日期與秒，若結束小於開始 → +1 天。

    回傳
    ----
    dict:
      {
        "LF1": {"爐號": str, "開始處理時間": Timestamp|None, "處理結束時間": Timestamp|None, "生產狀態": str, "停機時間": Timestamp|None},
        "LF2": {...}
      }
    """
    pool = pool or _POOL
    soup = _fetch_soup(URL_2143, pool)
    if soup is None:
        return {"ok": False, "reason": "連線逾時或頁面無資料"}
    if not now:
        now = pd.Timestamp.now()
    base = now.normalize()

    def get(id_):
        sp = soup.find("span", {"id": id_})
        return sp.text.strip() if sp else ""

    def _parse_time(dd_yy: str):
        if dd_yy == "":
            return None
        t = base + pd.to_timedelta(dd_yy + ':00')
        return t

    def _simple_adjust_cross(a, b):
        if a and b:
            b += pd.Timedelta(days=1) if a < b else b
        return b

    lf1_s = _parse_time(get("lblLf1_Stime"))
    # 防止讀取到的"開始處理時間"為前一天，造成「開始處理時間」、「處理結束時間」的日期錯誤
    # 目前暫時用「開始處理時間」與現在時間的差距是否超過10小時間判斷，並處理。
    if abs(now - lf1_s) > pd.Timedelta(hours=10):
        lf1_s -= pd.Timedelta(days=1)
    lf1_e = _simple_adjust_cross(lf1_s, _parse_time(get("lbllf1_Etime")))
    lf1_stop = None
    lf2_s = _parse_time(get("lbllf2_stime"))
    # 目前暫時用「開始處理時間」與現在時間的差距是否超過10小時間判斷，並處理。
    if abs(now - lf2_s) > pd.Timedelta(hours=10):
        lf1_s -= pd.Timedelta(days=1)
    lf2_e = _simple_adjust_cross(lf2_s, _parse_time(get("lbllf2_Etime")))

    lf2_stop = None
    data = {
        "LF1": {
            "爐號": get("lbllf1_heat"),
            "開始處理時間": lf1_s,
            "處理結束時間": lf1_e,
            "生產狀態": get("lblLF1sts"),
            "停機時間": lf1_stop,
        },
        "LF2": {
            "爐號": get("lbllf2_heat"),
            "開始處理時間": lf2_s,
            "處理結束時間": lf2_e,
            "生產狀態": get("lblLF2sts"),
            "停機時間": lf2_stop,
        },
    }
    return data

def _preprocess_schedule(raw_sched: List, is_2138: bool = True):
    """
    將「離散來源」整併成一張對齊的排程表，並補上「實際開始/實際結束」欄位。

    輸入
    ----
    raw_sched : list[tuple]
        由前置解析產生的紀錄清單。元素為
        (x座標, 開始時間, 結束時間, 爐號, 製程, 類別)
        其中「類別」為「表定 / 實際 / 輔助」，來自高度規則與 title 判斷。

    主要步驟
    --------
    1) 依製程與 x 座標、開始時間排序；做跨日展開：
        - 若同筆 end < start，視為跨日 +1 天。
        - 清晨視窗（<08:00）且與 now 差距大 → 全體 -1 天，否則首筆差距大 → +1 天。
        - 同一製程群組時間回捲 → +1 天。
    2) 分拆為三組 DataFrame：
        - planed：類別==「表定」的記錄，欄位改名為「表定開始時間/表定結束時間」
        - actual：類別==「實際」的記錄（「開始時間/結束時間」維持欄名）
        - aux   ：類別==「輔助」的記錄（僅 2138 的 EAFA/EAFB 會有）
    3) 「表定×實際」第一次合併（逐筆對應）：
        - 以（爐號, 製程）左連結，展開候選
        - 計算時間窗重疊量 overlap（>0 視為重疊），以及距離 distance
        - 以 has_overlap DESC, overlap_pos DESC, distance ASC 取最佳一筆
        - 命中者回寫至「實際開始時間/實際結束時間」
    4) 若是 is_2138=True，再用「輔助層」做第二次對齊（EAFA/EAFB）：
        - 以（製程）左連結 aux，重算 overlap/distance
        - 僅當第一階段已具「實際開始/實際結束」時，才用 aux 覆寫為更精準的時間窗
        - 目的是把 EAFA/EAFB 的綠色「送電刻度」時間窗對齊到相對的表定區段

    回傳
    ----
    pd.DataFrame
        欄位至少包含：
        ['爐號','製程','表定開始時間','表定結束時間','實際開始時間','實際結束時間']
        （呼叫端可再將欄位轉為「開始時間/結束時間」再行分類）

    備註
    ----
    - 本函式不做 past/current/future 的切桶；分類在上層以 now 與 2137/2143 狀態完成。
    - 合併策略允許「未重疊但最近」的匹配，以支援 MES 實務中偶發的剪裁或位移。
    """
    def _merge(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
            整合排程的表定、實際時間
        """
        # 以pre_merge_df 和 actual 的 ['爐號','製程'] 左連接，展開候選
        m = (
            df1.reset_index()      # 保存原索引，等一下回寫用的
            .merge(df2[['開始時間', '結束時間', '爐號', '製程']],
                   left_on=['爐號','製程'], right_on=['爐號','製程'], how='left')
        )

        # 計算時間窗重疊度: overlap = max(0, min(c,i) -max(b,h)
        start_max = m[['表定開始時間','開始時間']].max(axis=1)
        end_min = m[['表定結束時間', '結束時間']].min(axis=1)
        m['overlap_pos'] = (end_min - start_max).clip(lower=pd.Timedelta(0))
        m['distance'] = (start_max - end_min).clip(lower=pd.Timedelta(0))
        m['has_overlap'] = m['overlap_pos'] > pd.Timedelta(0)

        m = m.sort_values(['index', 'has_overlap', 'overlap_pos', 'distance'],
                          ascending=[True, False, False, True])
        # 對每筆pre_merge_df 挑 overlap 最大的那一筆 actual
        best = m.groupby('index', as_index=False).head(1)

        # 只要真的有候選 (「開始時間」,「結束時間」非NaT) 就回寫；允許「沒重疊但最近」
        hit = best['開始時間'].notna() & best['結束時間'].notna()

        # 等號右邊轉為 numpy，是為了避免標籤對齊，改用純位置寫入，穩定且更快速
        df1.loc[best.loc[hit, 'index'], ['實際開始時間', '實際結束時間']] = (
            best.loc[hit,['開始時間','結束時間']].to_numpy())

        # numpy 轉存進來的格式為是object，把格式由object 轉成datetime64[ns]
        df1[['實際開始時間', '實際結束時間']] = df1[['實際開始時間', '實際結束時間']].apply(pd.to_datetime)
        out = df1.copy()
        return out

    df = pd.DataFrame(raw_sched)

    # ------------ 排序、調整跨天 ---------------
    # by 製程，並依照x 座標排序、開始時間排程, 結束後轉為list
    # 等等需要再安排其它邏輯
    sorted_df = df.sort_values([4, 0, 1])

    sorted_list = list(sorted_df.itertuples(index=False, name=None))

    # 處理跨天後, 將結果轉為pd.DataFrame
    adjusted_cross_day_list = _adjust_cross_day(sorted_list, pd.Timestamp.now())
    adjusted_cross_day_df = pd.DataFrame(adjusted_cross_day_list)
    adjusted_cross_day_df.columns = ['x座標', '開始時間', '結束時間', '爐號', '製程', '類別']
    # ------------ 將離散的表定、實際、輔助記錄，配對及合併起來 ------------
    # 讀取記錄中有那些製程
    proc_set = set(pd.unique(adjusted_cross_day_df.loc[:,'製程'].dropna()))

    # 取出表定、實際、輔助的資料
    planed = adjusted_cross_day_df.loc[adjusted_cross_day_df['類別'].eq("表定")].copy()
    actual = adjusted_cross_day_df.loc[adjusted_cross_day_df['類別'].eq("實際")].copy()
    aux = adjusted_cross_day_df.loc[adjusted_cross_day_df['類別'].eq("輔助")].copy()

    pre_merge_df = planed.copy()
    pre_merge_df = pre_merge_df[['爐號','製程','開始時間','結束時間']]
    pre_merge_df = pre_merge_df.rename(columns={"開始時間": "表定開始時間", "結束時間": "表定結束時間"})
    pre_merge_df["實際開始時間"] = None
    pre_merge_df["實際結束時間"] = None

    # ["爐號","製程","開始時間","結束時間","實際開始時間","實際結束時間","預計完成時間","製程狀態"]

    # 確保時間是 datetime
    for col in ['表定開始時間','表定結束時間']: pre_merge_df[col] = pd.to_datetime(pre_merge_df[col])
    for col in ['開始時間','結束時間']: actual[col] = pd.to_datetime(actual[col])

    second_merge_df = _merge(pre_merge_df, actual)

    out = second_merge_df.copy()
    # ------- 解析2138 時，EAF lane 會有 aux 資料需要再處理 ------------
    if is_2138:
        final_merge_df = second_merge_df.copy()
        # 以final_merge_df 和 aux 的 ['製程'] 左連接，展開候選 (aux 沒有爐號)
        m = (
            final_merge_df.reset_index()  # 保存原索引，等一下回寫用的
            .merge(aux[['開始時間', '結束時間', '製程']],
                   left_on=['製程'], right_on=['製程'], how='left')
        )

        # 計算時間窗重疊度: overlap = max(0, min(c,i) -max(b,h)
        start_max = m[['表定開始時間', '開始時間']].max(axis=1)
        end_min = m[['表定結束時間', '結束時間']].min(axis=1)
        m['overlap_pos'] = (end_min - start_max).clip(lower=pd.Timedelta(0))
        m['distance'] = (start_max - end_min).clip(lower=pd.Timedelta(0))
        m['has_overlap'] = m['overlap_pos'] > pd.Timedelta(0)

        m = m.sort_values(['index', 'has_overlap', 'overlap_pos', 'distance'],
                          ascending=[True, False, False, True])
        # 對每筆pre_merge_df 挑 overlap 最大的那一筆 actual
        best = m.groupby('index', as_index=False).head(1)

        # 只要真的有候選 (「開始時間」,「結束時間」非NaT) 就回寫；允許「沒重疊但最近」
        # exclude the row with NaT at actual start and end time during 2nd merge
        hit = (best['開始時間'].notna() & best['結束時間'].notna() &
               best['實際開始時間'].notna() & best['實際結束時間'].notna())

        # 等號右邊轉為 numpy，是為了避免標籤對齊，改用純位置寫入，穩定且更快速
        final_merge_df.loc[best.loc[hit, 'index'], ['實際開始時間', '實際結束時間']] = (
            best.loc[hit, ['開始時間', '結束時間']].to_numpy())

        # numpy 轉存進來的格式為是object，把格式由object 轉成datetime64[ns]
        final_merge_df[['實際開始時間', '實際結束時間']] = (final_merge_df[['實際開始時間', '實際結束時間']]
                                                            .apply(pd.to_datetime))
        out = final_merge_df.copy()
    return out

def _lane_by_y(y_mid: float, fixed_lanes: Optional[Dict[str, Dict[str, float]]]) -> Optional[str]:
    """
    fixed_lanes 例：
    {
        "EAFA": {"min": 180, "max": 197},
        "EAFB": {"min": 220, "max": 237},
        "LF1-1": {"min": 250, "max": 268},
        "LF1-2": {"min": 286, "max": 304},
        "LF1": {"min": 288, "max": 309},
        "LF2": {"min": 330, "max": 342},
        "SCC1": {...}, ...
    }
    """
    if not fixed_lanes:
        return None
    for name, rng in fixed_lanes.items():
        y0, y1 = rng.get("min"), rng.get("max")
        if y0 is None or y1 is None:
            continue
        if y0 <= y_mid <= y1:
            return name
    return None

def _nearest_height_match(h: int, rule: Dict[str, Any]) -> Tuple[bool, int]:
    """
    回傳 (是否命中, |h - 最近允許高度|)；容忍 tol（預設 0）
    """
    cand = rule["heights"]
    tol = int(rule.get("tol", 0))
    # 最近距離
    d = min(abs(h - x) for x in cand) if cand else 999
    return (d <= tol, d)

def _classify_rectangle(
    page: str,
    coords: List[int],
    title: str,
    fixed_lanes: Optional[Dict[str, Dict[str, int]]] = None,
) -> RectClassify:
    """
    通用分類：
    - 先以 title 判斷是否「輔助」。
    - 再以高度（h = |y2-y1|）配合 page 規則分類 planned/actual/(actual_corrected)。
    - 若規則內含 only_in，需 lane 屬於指定集合才視為命中。
    - 無匹配則回傳 unknown。
    """
    page = "2138" if page.strip().startswith("2138") else ("2133" if page.strip().startswith("2133") else page.strip())
    x1,y1,x2,y2 = coords
    h = abs(y2 - y1)
    y_mid = (y1 + y2) / 2.0
    lane = _lane_by_y(y_mid, fixed_lanes)

    # 1) 輔助層
    if _AUX_TITLE_PAT.search(title or ""):
        return RectClassify(page, lane, "aux", "輔助", 1.0, f"title 命中輔助關鍵字；h={h}, lane={lane}")

    # 2) 依高度規則決定類別
    rules = _HEIGHT_RULES.get(page, {})
    best = None  # (kind, label, distance)
    for kind, rule in rules.items():
        ok, dist = _nearest_height_match(h, rule)
        if not ok:
            continue
        # 若規則限制 only_in（例如 2138 的校正實際只在 EAFA/EAFB），先檢核 lane 或 title
        allowed = rule.get("only_in")
        if allowed:
            lane_ok = (lane in allowed) if lane else False
            title_ok = any(k in (title or "") for k in allowed)
            if not (lane_ok or title_ok):
                # 沒有 lane 或 title 證據，就當沒命中
                continue
        # 取距離最小者（容忍度相同時無差）
        if (best is None) or (dist < best[2]):
            best = (kind, rule.get("label", kind), dist)

    if best:
        kind, label, dist = best
        # 距離 0 視為高信心；距離>0 視為中信心
        conf = 1.0 if dist == 0 else max(0.6, 1.0 - 0.1*dist)
        return RectClassify(page, lane, kind, label, conf, f"h={h} 命中 {page}.{kind} 規則；Δ={dist}; lane={lane}")

    # 3) 無匹配 → unknown
    return RectClassify(page, lane, "unknown", "未知", 0.0, f"h={h} 未命中任何 {page} 規則；lane={lane}")

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
    以分段線性插值將座標 x 映射到時間 t，含左右外插與除零防呆，並將結果四捨五入到秒。

    規則（依現況實作）
    ---------------
    - 前置：xs/ts 長度相同、已依 xs 遞增排序；ts 已做必要的跨日展開。
    - 左外插：沿第一段斜率外推；若第一段 dx=0，直接回傳 ts[0]。
    - 右外插：沿最後一段斜率外推；若最後一段 dx=0，直接回傳 ts[-1]。
    - 內插：在 [xs[j], xs[j+1]] 線性插值，j 由 bisect_right(xs, int(round(xq))) - 1 取得；
            若 dx=0（同 x），回傳左端時間 t0。
    - 所有回傳時間以 .round('S') 取整秒，避免浮點/微秒誤差。

    參數
    ----
    xq : float
        欲映射的 x。
    xs : list[int]
        節點 x（遞增）。
    ts : list[pd.Timestamp]
        節點 t（建議單調且已展開）。

    回傳
    ----
    pd.Timestamp
        對應時間（四捨五入到秒）。
    """
    n = len(xs)
    if n == 0:
        return pd.Timestamp.now().round('S')
    if n == 1:
        return ts[0].round('S')

    # 左端外插
    if xq <= xs[0]:
        i = 1
        while i < n and xs[i] == xs[0]:
            i += 1
        if i == n:
            return ts[0].round('S')
        dt = ts[i] - ts[0]
        dx = xs[i] - xs[0]
        if dx == 0:
            return ts[0].round('S')
        return (ts[0] + (xq - xs[0]) * dt / dx).round('S')

    # 右端外插
    if xq >= xs[-1]:
        i = n - 2
        while i >= 0 and xs[i] == xs[-1]:
            i -= 1
        if i < 0:
            return ts[-1].round('S')
        dt = ts[-1] - ts[i]
        dx = xs[-1] - xs[i]
        if dx == 0:
            return ts[-1].round('S')
        return (ts[-1] + (xq - xs[-1]) * dt / dx).round('S')

    # 內插
    import bisect
    j = bisect.bisect_right(xs, int(round(xq))) - 1
    x0, x1 = xs[j], xs[j+1]
    t0, t1 = ts[j], ts[j+1]
    dx = x1 - x0
    if dx == 0:
        return t0.round('S')
    w = (xq - x0) / dx
    return (t0 + (t1 - t0) * w).round('S')



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
    """
    調整跨日情境下的開始/結束時間，使序列在時間軸上自然遞增與就近對齊「now」。

    規則（依現況實作）
    ---------------
    - 若 end < start 視為跨日：end += 1 天（必要時 start 亦 +1 天）。
    - 清晨視窗或第一筆與 now 差距過大：視情況將整段或首筆 +1/-1 天，使資料落在正確日期側。
    - 以「同群組（grp=爐號+製程）」為單位，如遇到「時間回捲」（當前 start < 前一筆 start），則 +1 天。
    - 回傳的 tuple 兼容兩種形狀：
        舊版： (x, start, end, 爐號, 製程)
        新版： (x, start, end, 爐號, 製程, label)  # 多一個 label（例如 表定/實際/輔助）

    參數
    ----
    records : list[tuple]
        已排序去重後的排程清單，元素至少包含 x、start、end、爐號、製程，可能還有 label。
    now : datetime
        分類與展開的參考時間（建議使用同一時區/naive）。

    回傳
    ----
    list[tuple]
        經跨日調整後的清單；若輸入有第 6 欄 label，輸出會保留該欄不變。

    備註
    ----
    - 若 now 與 start/end 的 tz 屬性不一致（naive/aware 混用）會造成 pandas 的比較錯誤，呼叫前請先統一。
    """

    adjusted = list(records)

    def unify(proc):
        return "EAF" if proc in ("EAFA", "EAFB") else proc

    first_seen_done = set()         # 記錄每個「製程群組」是否已處理過第一筆
    last_start_by_group = {}        # 記錄各群組上一筆 start，用於偵測回捲

    #for i, (x, start, end, furnace, proc, label) in enumerate(adjusted):
    for i, item in enumerate(adjusted):
        # 新版scrape_schedule 呼叫時，會有6 個元素，舊版有5個。所以改成這樣以便相容
        x , start, end, furnace, proc, *rest = item
        if rest:
            label = rest[0]

        # MES rectangles wrap at 00:00 so end might be earlier than start.
        #grp = unify(proc)
        grp = proc

        if end < start:
            end += pd.Timedelta(days=1)

        # Special handling for early‑morning viewing window (<08:00)
        if now.time() < datetime.strptime("08:00", "%H:%M").time():
            if abs(now - start) > pd.Timedelta(hours=10):
                start -= pd.Timedelta(days=1)
                end -= pd.Timedelta(days=1)

        #elif i == 0 and abs(now - start) > pd.Timedelta(hours=10):
        elif (grp not in first_seen_done) and abs(now - start) > pd.Timedelta(hours=10):
            start += pd.Timedelta(days=1)
            end += pd.Timedelta(days=1)
            first_seen_done.add(grp)

        # 4) 同一製程群組若時間回捲（比前一筆還早），視為跨日，+1 天
        prev_start = last_start_by_group.get(grp)
        if prev_start is not None and start < prev_start:
            start += pd.Timedelta(days=1)
            end   += pd.Timedelta(days=1)

        last_start_by_group[grp] = start

        # 新版scrape_schedule 呼叫時，會有6 個元素，舊版有5個。所以改成這樣以便相容
        if rest:
            adjusted[i] = (x, start, end, furnace, proc, label)
        else:
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
    scrape_schedule()

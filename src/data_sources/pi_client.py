from __future__ import annotations
from functools import lru_cache
from typing import Iterable, Literal, Dict, Optional
import pandas as pd
import PIconnect as Pi
import numpy as np
from src.logging_utils import get_logger, log_exceptions

logger = get_logger(__name__)

SummaryType = Literal["RANGE", "MAXIMUM", "MINIMUM", "AVERAGE","TOTAL"]

def _normalize_raw_values(raw_dict: dict) -> dict:
    """
    將 raw_dict 中的原始值轉為 float 或 None，以便後續轉為數值型態。

    處理規則：
      1. 枚舉型 (AFEnumerationValue)：記 WARNING，設為 None。
      2. 字串 (例如 'Bad', 'OFF', 'N/A')：記 WARNING，設為 None。
      3. None 或 np.nan：保留。
      4. 其他類型 (int, float, 數字字串)：保留，留待 pd.to_numeric 處理。

    Args:
        raw_dict (dict): key 為 tag name，value 為 原始 p.current_value。

    Returns:
        dict: 處理後的字典，所有無法轉為 float 的值皆為 None。
    """
    for tag_name, val in raw_dict.items():
        # 1) 先檢查「枚舉型」(AFEnumerationValue)，它會有 .Name 和 .Value 屬性
        if hasattr(val, 'Name') and hasattr(val, 'Value'):
            logger.warning(f"[PIClient] Tag '{tag_name}' 回傳枚舉型 (Enumeration)：{val} → 以 NaN 處理")
            raw_dict[tag_name] = None
            continue

        # 2) 如果 val 是字串 (例如 'Bad'、'OFF'、'N/A'...)
        if isinstance(val, str):
            logger.warning(f"[PIClient] Tag '{tag_name}' 回傳字串：'{val}' → 以 NaN 處理")
            raw_dict[tag_name] = None
            continue

        # 3) 如果 val 本身就是 None 或 np.nan，就跳過（後面 to_numeric 會自動轉成 NaN）
        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue

        # 4) 到這裡 val 很可能是 int、float、也可能是「看起來像數字的字串」→ 交給 to_numeric 處理

    return raw_dict

class PIClient:
    """
    封裝 PIconnect 取數邏輯，提供即時值與歷史統計查詢功能。

    Attributes:
        SUMMARY_MAP (Dict[SummaryType, int]):
            SummaryType 到 PIconnect.SummaryType 常數的對應表。
        _point_cache (Dict[str, Pi.PIPoint]):
            已搜尋到的 PIPoint 快取，用於減少搜尋次數。
    """

    SUMMARY_MAP: Dict[SummaryType] = {
        "RANGE": Pi.PIConsts.SummaryType.RANGE,
        "MAXIMUM": Pi.PIConsts.SummaryType.MAXIMUM,
        "MINIMUM": Pi.PIConsts.SummaryType.MINIMUM,
        "AVERAGE": Pi.PIConsts.SummaryType.AVERAGE,
        "TOTAL": Pi.PIConsts.SummaryType.TOTAL,
    }

    def __init__(self, timezone: str = "Asia/Taipei"):
        """
        初始化 PIClient 並設定 PIConfig 時區。

        Args:
            timezone (str):
                PIConfig 使用的時區字串，例如 "Asia/Taipei"，預設為 GMT+8。
        """
        Pi.PIConfig.DEFAULT_TIMEZONE = timezone
        self._point_cache: Dict[str, Pi.PIPoint] = {}

    # ---- 單一 tag，仍保留 LRU ----
    @lru_cache(maxsize=256)
    def _search_point(self, tag: str) -> Pi.PIPoint | None:
        """
        單一 tag 搜尋 PIPoint，並以 LRU 快取結果。

        Args:
            tag (str): PI tag 名稱。

        Returns:
            Pi.PIPoint | None: 找到的 PIPoint 或 None（若搜尋失敗）。

        Logs:
            ERROR: 搜尋例外時記錄錯誤。
        """
        try:
            with Pi.PIServer() as server:
                return server.search(tag)[0]
        except Exception as e:
            logger.error('單點搜尋失敗 %s : %s', tag, e)
            return None

    # ---- 多 tag 查詢 ----
    def search_points(self, tags: Iterable[str]) -> Dict[str, Pi.PIPoint]:
        """
        批次搜尋多個 tags 的 PIPoint，並回傳一個字典。

        只針對快取中不存在的 tag 執行搜尋，成功後存入快取。

        Args:
            tags (Iterable[str]): 要搜尋的 tag 名稱列表或其他可疊代結構。

        Returns:
            Dict[str, Pi.PIPoint]: 搜尋成功的 tag->PIPoint 映射，失敗的 tag 則不包含於結果中。
        """
        tags = list(tags)   # 將傳入的Iterable[str} (可能是generator,set,Index)轉成可重複使用的list
        result: Dict[str, Pi.PIPoint] = {}
        for tag in tags:
            # 先試從 cache（_search_point 本身也快取）
            point = self._point_cache.get(tag)
            if point is None:
                # 再呼一次底層搜尋（帶快取）
                point = self._search_point(tag)
                if point:
                    self._point_cache[tag] = point
                else:
                    # 這裡可以決定是跳過、raise 或是用 dummy point
                    continue
            result[tag] = point
        return result

    # ---------------------------------------
    # 即時值
    # ---------------------------------------
    @log_exceptions(logger)     # 若發生未捕捉例外，自動寫 ERROR log + stacktrace
    def current_values(self, tags: Iterable[str]) -> pd.Series:
        """
        查詢多個 tags 的即時值並回傳 pd.Series。

        行為流程：
          1. 搜尋 PIPoint。
          2. 取得 current_value。
          3. 呼叫 _normalize_raw_values 處理非數值型態。
          4. 轉為 float，無法轉型者以 NaN 取代。
          5. 記錄被強制轉 NaN 的 tag 名稱及原始值。

        Args:
            tags (Iterable[str]): 要查詢的 tag 名稱列表。

        Returns:
            pd.Series: 索引為 tag 名稱，值為當前值 (float)，名稱為 "current_value"。
        """
        # 1) 如果遲線失敗，pts 就會是空字典 {}
        pts = self.search_points(tags)

        # 2) 先把「原始值」收齊
        raw = {t: p.current_value for t, p in pts.items()}

        # 3) 屬性/字串檢查，非數值一律轉成None
        raw = _normalize_raw_values(raw)

        # 4) 逐一轉float，失敗就Nan
        numeric = pd.to_numeric(pd.Series(raw), errors="coerce")

        # 5) 找出被轉成 NaN 的項目（且原本不是 NaN / None）
        mask = numeric.isna() & pd.Series(raw).notna()
        if mask.any():
            logger.warning(
                "Coerced %d / %d tags to NaN → %s",
                mask.sum(), len(numeric),
                ", ".join(f"{t} = {raw[t]}" for t in numeric[mask].index)
            )
            """
            例如：
            Coerced 2 / 25 tags to NaN → EAFA_Power='OFF', EAFB_Pressure='N/A'
            """

        numeric.name = "current_value"
        return numeric

    def query(
        self,
        st: pd.Timestamp,
        et: pd.Timestamp,
        tags: Iterable[str],
        summary: SummaryType = "RANGE",
        interval: str = "15m",
        fillna_method: Optional[str] = None,
        tz_offset_sec: int = 0,
    ) -> pd.DataFrame:
        st, et = [t - pd.offsets.Second(tz_offset_sec) for t in (st, et)]
        code = self.SUMMARY_MAP[summary]
        """
        查詢多個 tags 的歷史統計資料並回傳 DataFrame。

        Args:
            st (pd.Timestamp): 查詢起始時間（含）。
            et (pd.Timestamp): 查詢結束時間（含）。
            tags (Iterable[str]): 要查詢的 tag 名稱列表。

        Keyword Args:
            summary (SummaryType): 統計類型，可為 'RANGE', 'MAXIMUM', 'MINIMUM', 'AVERAGE', 'TOTAL'。
            interval (str): 時段粒度字串，例如 '15m'、'1h'。
            fillna_method (Optional[str]): 缺失值填補方法，支援 'ffill' 或 'bfill'。
            tz_offset_sec (int): 欲調整的時區秒差，預設 0。

        Returns:
            pd.DataFrame: 索引為時間 (datetime)，欄位為 tags，值為指定 summary 的 float。

        Raises:
            無：本方法不會主動拋例外，若搜尋點失敗，該 column 會被跳過。
        
        備註：
            本方法將略過搜尋失敗或 summaries 失敗的tag
        """
        dfs = []
        for tag in tags:
            point = self._search_point(tag)
            df = point.summaries(st, et, interval, code)                # 1
            dfs.append(pd.to_numeric(df[summary], errors="coerce"))     # 2

        raw = pd.concat(dfs, axis=1)                # 3
        raw.index = raw.index.tz_localize(None) + pd.offsets.Second(tz_offset_sec)  # 4
        raw.columns = list(tags)                    # 5

        if fillna_method in ("ffill", "bfill"):     # 6
            raw = getattr(raw, fillna_method)()

        return raw

if __name__ == "__main__":  # pragma: no cover  # 測試用，正式執行不跑
    client = PIClient()

    # 假設其中一個 Tag 會回傳 "OFF" → 會被記 WARNING
    demo_tags = ["W511_MS1/22.8KV/AJ_270/P", "W512_FT-401.PV", "W512_FT-202.PV"]
    print(client.current_values(demo_tags))
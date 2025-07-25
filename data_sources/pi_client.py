from __future__ import annotations
from functools import lru_cache
from typing import Iterable, Literal, Dict, Optional
import pandas as pd
import PIconnect as Pi
import numpy as np
from logging_utils import get_logger, log_exceptions

logger = get_logger(__name__)

SummaryType = Literal["RANGE", "MAXIMUM", "MINIMUM", "AVERAGE","TOTAL"]

def _normalize_raw_values(raw_dict: dict) -> dict:
    """
    把 raw_dict 裡的 p.current_value 做「屬性/字串檢查」，
    只要是 AFEnumerationValue、'Bad'、其他非 float 字串等通通先設成 None，
    並分別在 logger.warning 裡記錄是哪種情況。
    其餘能直接轉 float 的留待 pd.to_numeric 處理。
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
    """封裝 PIconnect 取數；任何 UI / service 僅呼叫這層。"""

    SUMMARY_MAP: Dict[SummaryType] = {
        "RANGE": Pi.PIConsts.SummaryType.RANGE,
        "MAXIMUM": Pi.PIConsts.SummaryType.MAXIMUM,
        "MINIMUM": Pi.PIConsts.SummaryType.MINIMUM,
        "AVERAGE": Pi.PIConsts.SummaryType.AVERAGE,
        "TOTAL": Pi.PIConsts.SummaryType.TOTAL,
    }

    def __init__(self, timezone: str = "Asia/Taipei"):
        # 從 PI 取出的 timestamp 時區改成 GMT+8
        Pi.PIConfig.DEFAULT_TIMEZONE = timezone
        self._point_cache: Dict[str, Pi.PIPoint] = {}

    # ---- 單一 tag，仍保留 LRU ----
    @lru_cache(maxsize=256)
    def _search_point(self, tag: str) -> Pi.PIPoint | None:
        """
            單一 tag 搜尋，並快取PIPoint。
            如果失敗，log 後回傳 None。
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
        一次性搜尋多個tag 的PIPoint 物件，並回傳一個字典。
        但只對 self._point_cache 裡沒有的做搜尋，並對每一筆失敗個別處理。
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
        將PIPoint 物件中的屬性 (current_value PI Data Archive 發出一次性查詢)，
        把該結果的資料型態嘗試從 object->float，若遇文字型態，則用Nan 取代。
        凡是無法轉型的原始值，都記一條 WARNING log。
        最終以 pd.Series 回傳。
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

        # -- 尋點 & 取數 ----------------------------------------------------
        """
            1.針對每一個PIPoint 透過 summaries 的方法，依code 內容，決定特定區間取出值為何種形式。回傳的資料為DataFrame 型態
            2.將資料型態從Object -> float，若有資料中有文字無法換的，則用NaN 缺失值取代。
              如果 df shape(資料筆數,PIPoint 數量)->(資料筆數,1), 那pd.to_numeric 後會變成pd.Series
            3.將list 中所有的 DataFrame 合併為一組新的 DataFrame 資料
            4.把原本用來做index 的時間，將時區從tz aware 改為 native，並加入與OSAKI 時間差參數進行調整。
            5.將結果直接用tag 當欄名，以DataFrame 格式回傳。 shape(資料數量, tag數量)
            6.決定是否填補nan 值
        Parameters
        ----------
        fillna_method : str
            收斂空值方法，支援 "ffill" / "bfill" / "None"。
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
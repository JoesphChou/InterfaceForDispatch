from __future__ import annotations
from functools import lru_cache
from typing import Iterable, Literal, Dict
import pandas as pd
import PIconnect as Pi

SummaryType = Literal["RANGE", "MAXIMUM", "MINIMUM", "AVERAGE"]

class PIClient:
    """封裝 PIconnect 取數；任何 UI / service 僅呼叫這層。"""

    SUMMARY_MAP: Dict[SummaryType] = {
        "RANGE": Pi.PIConsts.SummaryType.RANGE,
        "MAXIMUM": Pi.PIConsts.SummaryType.MAXIMUM,
        "MINIMUM": Pi.PIConsts.SummaryType.MINIMUM,
        "AVERAGE": Pi.PIConsts.SummaryType.AVERAGE,
        "TOTAL": Pi.PIConsts.SummaryType.TOTAL,
        "ALL": Pi.PIConsts.SummaryType.ALL,
    }

    def __init__(self, timezone: str = "Asia/Taipei"):
        # 從 PI 取出的 timestamp 時區改成 GMT+8
        Pi.PIConfig.DEFAULT_TIMEZONE = timezone

    # ---- (1) 單一 tag，仍保留 LRU ----
    @lru_cache(maxsize=256)
    def _search_point(self, tag: str):
        """只做一次搜尋並快取 PIPoint（連線只用一次）"""
        with Pi.PIServer() as server:
            return server.search(tag)[0]

    def search_points(self, tags: Iterable[str]) -> Dict[str, Pi.PIPoint]:
        """
        一次性搜尋多個tag 的PIPoint 物件，並回傳一個字典。
        """
        tags = list(tags)   # 將傳入的Iterable[str} (可能是generator,set,Index)轉成可重複使用的list
        with Pi.PIServer() as server:                 # **只開一次連線**
            points = server.search(tags)              # PIconnect 支援 list/tuple
        # points 與 tags 順序一致
        return dict(zip(tags, points))

    def current_values(self, tags: Iterable[str]) -> pd.Series:
        """
        將PIPoint 物件中的屬性 (current_value PI Data Archive 發出一次性查詢)，把該結果的資料型態 object->float，
        若遇文字型態，則用Nan 取代。最終以 pd.Series 回傳。
        """
        pts = self.search_points(tags)
        return pd.Series({t: pd.to_numeric(p.current_value, errors='coerce') for t, p in pts.items()},
                         name="current_value")

    def query(
        self,
        st: pd.Timestamp,
        et: pd.Timestamp,
        tags: Iterable[str],
        summary: SummaryType = "RANGE",
        interval: str = "15m",
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
        """
        dfs = []
        for tag in tags:
            point = self._search_point(tag)
            df = point.summaries(st, et, interval, code)    #1
            dfs.append(pd.to_numeric(df[summary], errors="coerce"))     #2

        raw = pd.concat(dfs, axis=1)    #3
        raw.index = raw.index.tz_localize(None) + pd.offsets.Second(tz_offset_sec)  #4
        raw.columns = list(tags)    #5
        return raw
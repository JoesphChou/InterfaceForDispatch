from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Literal, Dict, Optional

import numpy as np
import pandas as pd

from src.logging_utils import get_logger, log_exceptions


logger = get_logger(__name__)

# 和實際 pi_client.py 對齊的摘要型別（實務上也會用到 TOTAL / ALL）
SummaryType = Literal["RANGE", "MAXIMUM", "MINIMUM", "AVERAGE", "TOTAL", "ALL"]


def _stable_seed(obj) -> int:
    """
    產生跨行程穩定的隨機種子。

    Parameters
    ----------
    obj :
        任意可 repr() 的物件，會被轉成字串後做雜湊。

    Returns
    -------
    int
        介於 [0, 2**64) 的整數種子。
    """
    import hashlib

    h = hashlib.sha256(repr(obj).encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def _normalize_raw_values(raw_dict: dict) -> dict:
    """
    把 raw_dict 裡的「原始 current_value」做一次預清理。

    真實版會處理 AFEnumerationValue、"Bad" 等特殊型別；
    在 mock 版中我們仍保留介面與行為：
    - 非數值、可疑字串會被轉成 None
    - 其餘交由 `pd.to_numeric(..., errors="coerce")` 處理

    Parameters
    ----------
    raw_dict : dict
        {tag_name: value} 的字典。

    Returns
    -------
    dict
        清理後的字典；保留 key，不可轉型的值會改為 None。
    """
    for tag_name, val in list(raw_dict.items()):
        # 在 mock 中我們不會真的遇到 AFEnumerationValue，
        # 但保留這層結構以對齊真實版本。
        # 這裡只簡單處理字串與明確的 None/NaN。
        if isinstance(val, str):
            logger.warning(
                "[MockPIClient] Tag '%s' 回傳字串 '%s' → 以 NaN 處理",
                tag_name,
                val,
            )
            raw_dict[tag_name] = None
            continue

        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue

    return raw_dict


class PIClient:
    """
    離線用的 Mock PIClient。

    此類別刻意模仿實際 `pi_client.PIClient` 的介面與回傳結構，
    讓 main.py 等呼叫端可以在「不連線 PI」的情況下照常運作。

    - current_values(tags) → pd.Series
        index 為 tag 名稱，dtype=float64，name="current_value"。
    - query(st, et, tags, summary, interval, tz_offset_sec) → pd.DataFrame
        index = DatetimeIndex（左閉右開 [start, end) 的「起始時間」）、columns=tag 名稱。
    """

    # 實際端會映射到 PIconnect 的 SummaryType；
    # 在 mock 版僅保留 key，以便呼叫端使用相同常數。
    SUMMARY_MAP: Dict[SummaryType, SummaryType] = {
        "RANGE": "RANGE",
        "MAXIMUM": "MAXIMUM",
        "MINIMUM": "MINIMUM",
        "AVERAGE": "AVERAGE",
        "TOTAL": "TOTAL",
        "ALL": "ALL",
    }

    def __init__(self, timezone: str = "Asia/Taipei"):
        # 真實端會設定 Pi.PIConfig.DEFAULT_TIMEZONE；
        # 在 mock 版僅保留屬性以便除錯與顯示。
        self.timezone = timezone

    # ---------------------------------------
    # 尋點（介面相容用，實際不連線）
    # ---------------------------------------
    def search_points(self, tags: Iterable[str]) -> Dict[str, object]:
        """
        一次性搜尋多個 tag 的「模擬 PIPoint」。

        真實版會回傳 PIconnect.PIPoint；
        mock 版回傳擁有 .current_value 屬性的簡單物件。

        Parameters
        ----------
        tags : Iterable[str]
            要查詢的點名清單。

        Returns
        -------
        Dict[str, object]
            {tag: dummy_point}，每個 dummy_point 具備 .current_value 屬性。
        """
        tags = list(tags)
        pts: Dict[str, object] = {}

        # 使用穩定種子確保離線測試可重現
        seed = _stable_seed(("search_points", tuple(sorted(tags))))
        rng = np.random.default_rng(seed)

        class _DummyPoint:
            __slots__ = ("current_value",)

            def __init__(self, value: float):
                self.current_value = float(value)

        values = rng.uniform(10.0, 100.0, size=len(tags))
        for tag, val in zip(tags, values):
            pts[tag] = _DummyPoint(val)

        return pts

    # 保留與真實版相同的 LRU 介面；實作上只回傳 object。
    @lru_cache(maxsize=256)
    def _search_point(self, tag: str):
        """
        在真實版會搜尋單一 PIPoint 並快取；
        在 mock 版僅回傳一個 placeholder 物件，避免 AttributeError。
        """
        return object()

    # ---------------------------------------
    # 即時值
    # ---------------------------------------
    @log_exceptions(logger)  # 若發生未捕捉例外，自動寫 ERROR log + stacktrace
    def current_values(self, tags: Iterable[str]) -> pd.Series:
        """
        將模擬的 PIPoint.current_value 收集成一個 Series，並嘗試轉成 float。

        此函式刻意模仿真實 `PIClient.current_values()` 的行為：
        - 先透過 search_points() 取得「點位物件」
        - 讀取 .current_value，放進 dict
        - 呼叫 _normalize_raw_values() 做預清理
        - 最後用 pd.to_numeric(..., errors="coerce") 轉成 float

        Parameters
        ----------
        tags : Iterable[str]
            要查詢即時值的點名清單。

        Returns
        -------
        pandas.Series
            index 為 tag 名稱、dtype=float64，name="current_value"。
        """
        tags = list(tags)
        if not tags:
            return pd.Series(dtype=float, name="current_value")

        pts = self.search_points(tags)
        raw = {t: p.current_value for t, p in pts.items()}

        raw = _normalize_raw_values(raw)
        numeric = pd.to_numeric(pd.Series(raw), errors="coerce")

        mask = numeric.isna() & pd.Series(raw).notna()
        if mask.any():
            logger.warning(
                "[MockPIClient] Coerced %d / %d tags to NaN → %s",
                mask.sum(),
                len(numeric),
                ", ".join(f"{t}={raw[t]!r}" for t in numeric[mask].index),
            )

        numeric.name = "current_value"
        return numeric

    # ---------------------------------------
    # 區間摘要（summary）
    # ---------------------------------------
    def query(
        self,
        st: pd.Timestamp,
        et: pd.Timestamp,
        tags: Iterable[str],
        summary: SummaryType = "RANGE",
        interval: str = "15m",
        tz_offset_sec: int = 0,
        fillna_method: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        模擬 PI 的 summaries(st, et, interval, summary) 行為。

        重要設計（對齊你剛才描述的行為）：
        - 時間戳代表「區間起始時間」
        - 區間定義為左閉右開：[t, t + interval)
        - 對於整天區間，例如
            st = 2025-11-13 00:00:00
            et = 2025-11-14 00:00:00
            interval = "15m"
          會產生 96 筆，從 00:00, 00:15, ..., 23:45

        Parameters
        ----------
        st, et : pandas.Timestamp
            查詢起訖時間。
        tags : Iterable[str]
            要查詢的點名清單。
        summary : SummaryType, default "RANGE"
            區間摘要型別（與真實端使用的字串一致）。
        interval : str, default "15m"
            pandas offset 字串，例如 "8s", "15m", "15min", "1h"。
        tz_offset_sec : int, default 0
            時區偏移秒數；會在內部先減去、最後加回，以模擬真實端的邏輯。

        Returns
        -------
        pandas.DataFrame
            index 為 DatetimeIndex、columns 為 tag 名稱、dtype=float64。
        """
        tags = list(tags)
        if not tags:
            return pd.DataFrame(dtype="float64")

        # 先扣掉 offset，在本地時間軸上做對齊
        st_adj = pd.Timestamp(st) - pd.offsets.Second(tz_offset_sec)
        et_adj = pd.Timestamp(et) - pd.offsets.Second(tz_offset_sec)

        # ---- 正規化 interval，避免 '15m' 被當成 MonthEnd ----
        _interval_in = str(interval).strip()
        if _interval_in.lower().endswith("m") and not _interval_in.lower().endswith("min"):
            # '15m' / '15M' → '15min'
            num = _interval_in[:-1]
            interval_norm = f"{num}min"
        else:
            interval_norm = _interval_in

        off = pd.tseries.frequencies.to_offset(interval_norm)
        if not hasattr(off, "delta"):
            raise ValueError(
                f"interval={interval!r} 解析為非固定頻率 {off!r}；"
                "請改用秒/分/小時等固定頻率（例如 '8s', '15min', '1h'）。"
            )
        off_str = off.freqstr  # 例如 '15min'
        step = pd.Timedelta(off_str)

        # ---- 建立左閉右開 [start, end) 的「起始時間」格點 ----
        start_aligned = pd.to_datetime(st_adj).floor(off_str)
        end_floor = pd.to_datetime(et_adj).floor(off_str)

        # 最後一個「起始時間」必須 < et_adj
        # 對於整天：st=00:00, et=翌日00:00, interval=15min
        # start_aligned=00:00, end_floor=翌日00:00 → last_point = 23:45
        last_point = end_floor - step

        if last_point < start_aligned:
            idx = pd.DatetimeIndex([], dtype="datetime64[ns]")
        else:
            idx = pd.date_range(start=start_aligned, end=last_point, freq=off_str)

        # 加回 tz_offset，並確保 tz-naive
        idx = idx.tz_localize(None) + pd.offsets.Second(tz_offset_sec)

        if len(idx) == 0:
            return pd.DataFrame(index=idx, columns=tags, dtype="float64")

        # ---- 依 summary 型別產生模擬數值（結構與 dtype 對齊即可）----
        seed = _stable_seed(
            ("query", tuple(sorted(tags)), str(st_adj), str(et_adj), summary, off_str)
        )
        rng = np.random.default_rng(seed)

        # 平滑基底：用 sin 波 + 雜訊
        t = np.linspace(0.0, 2.0 * np.pi, len(idx), endpoint=False)
        base_mean = 50.0 + 10.0 * np.sin(t)
        noise = rng.normal(0.0, 1.5, size=len(idx))

        def make_base() -> np.ndarray:
            return (base_mean + noise).astype("float64").clip(min=0.0)

        def make_for_summary(sum_kind: SummaryType) -> np.ndarray:
            if sum_kind == "AVERAGE" or sum_kind == "ALL":
                return make_base()
            elif sum_kind == "MAXIMUM":
                return make_base() + np.abs(rng.normal(2.0, 1.0, size=len(idx)))
            elif sum_kind == "MINIMUM":
                return (make_base() - np.abs(rng.normal(2.0, 1.0, size=len(idx)))).clip(
                    min=0.0
                )
            elif sum_kind == "RANGE":
                high = make_base() + np.abs(rng.normal(3.0, 1.0, size=len(idx)))
                low = (make_base() - np.abs(rng.normal(3.0, 1.0, size=len(idx)))).clip(
                    min=0.0
                )
                return (high - low).clip(min=0.0)
            elif sum_kind == "TOTAL":
                seconds = pd.Timedelta(off_str).total_seconds()
                return make_base() * float(seconds)
            else:
                return make_base()

        # 為每個 tag 產生一組數值，加一點 tag-specific 微調（避免全欄位完全一樣）
        data = {}
        for i, tag in enumerate(tags):
            vals = make_for_summary(summary)
            data[tag] = (vals + i * 0.3).astype("float64")

        df = pd.DataFrame(data, index=idx)
        df = df.astype("float64")
        return df


if __name__ == "__main__":  # pragma: no cover
    # 簡單自我測試：確認結構與 dtype
    client = PIClient()

    demo_tags = ["2H120", "TG1 NG", "BFG#1"]
    print("=== current_values ===")
    s = client.current_values(demo_tags)
    print(s, s.dtype, s.name)

    st = pd.Timestamp("2025-11-13 00:00:00")
    et = pd.Timestamp("2025-11-14 00:00:00")

    for summary in ["AVERAGE", "MAXIMUM", "MINIMUM", "RANGE", "TOTAL", "ALL"]:
        df = client.query(st, et, demo_tags, summary=summary, interval="15m")
        print(f"=== query summary={summary} ===")
        print(df.head())
        print("shape:", df.shape, "| index[0]:", df.index[0] if len(df) else None)
        assert list(df.columns) == demo_tags
        assert str(df.index.dtype) == "datetime64[ns]"
        if len(df):
            assert all(df.dtypes == "float64")
            if summary == "RANGE":
                # 整天 + 15m → 應為 96 筆（左閉右開）
                assert len(df) == 96, f"RANGE length expected 96, got {len(df)}"

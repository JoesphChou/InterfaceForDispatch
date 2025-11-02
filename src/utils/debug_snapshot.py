"""
utils/debug_snapshot.py

提供 export_debug_snapshot()，
用來把目前關鍵 DataFrame / Series 抽樣存成小型測試資料，
寫到 tests/data/ 目錄，日後可直接拿來做 CI 的整合測試。

使用時機：
- 你在本機（或公司環境）跑 GUI / 主程式後，手動呼叫一次：
    from utils.debug_snapshot import export_debug_snapshot
    export_debug_snapshot(main_window=self)
  這樣就能把當下狀態輸出成小樣本檔案，放進 repo 的 tests/data/
"""

import os
import datetime
import pandas as pd

from src.utils.sample_io import save_sample_df


def _ensure_dir(path: str) -> None:
    """確保資料夾存在。例如 tests/data/"""
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)


def export_debug_snapshot(
    main_window=None,
    pi_client=None,
    out_dir: str = "tests/data",
    max_rows: int = 500,
    round_ts: str = "15min",
):
    """
    匯出目前系統的重要資料快照到 tests/data/，用於之後寫測試。

    參數：
    - main_window: 你的主視窗物件 (例如 MyMainWindow 實例)。
      目標是可以從它身上拿到「已經算好的 DataFrame」，
      例如:
        main_window.current  -> 當前儀表板/即時資訊 (可能是 DataFrame 或 Series)
        main_window.df_benefit -> 效益表的 DataFrame
        main_window.schedule_df -> 排程相關的 DataFrame (schedule_2133 的整合結果)
      如果你的屬性名稱不一樣，記得改下面的 candidates_from_ui 那段。

    - pi_client: (可選) 連 PI 系統的 client 物件，如果你想在這裡主動再抓幾個 tag。
      如果你不想連 PI（例如公司內網機密），可以傳 None。
      建議：在公司機器上呼叫時才丟進來，在家就不要給它。

    - out_dir: 匯出的資料夾（預設 tests/data）
    - max_rows: 每個樣本最多保留幾列
    - round_ts: 要不要把 timestamp floor 到某個粒度
                (方便匿名/壓縮，例如 15min 等級已足夠重現邏輯)

    產出：
    - tests/data/snapshot_YYYYMMDD_HHMM/<很多 .parquet or .csv>
      檔名會依資料來源決定
    """

    # 時間戳，讓每次匯出有獨立資料夾
    ts_label = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    snapshot_dir = os.path.join(out_dir, f"snapshot_{ts_label}")
    os.makedirs(snapshot_dir, exist_ok=True)

    # ------------------------------------------------------------
    # 1. 從 UI / 主程式物件抓到的重要中間結果
    #    你可以依你的 main.py 實際屬性來調整
    # ------------------------------------------------------------
    candidates_from_ui = {}

    if main_window is not None:
        # 下面是範例命名。請依你的 MyMainWindow 實際屬性調整：
        #
        # 假設：
        #   self.current -> 目前儀表板用到的 Series/DataFrame
        #   self.df_benefit -> 效益計算結果 (tableWidget_4/tableWidget_5 來源)
        #   self.schedule_df -> 排程資料（經 schedule_scraper 清過、跨日修正後的）
        #   self.version_used -> 各版本/單價/tooltip 資訊 (dict or DataFrame)
        #
        # 沒有的就拿掉，有的就加進來。

        if hasattr(main_window, "current"):
            candidates_from_ui["current"] = getattr(main_window, "current")

        if hasattr(main_window, "df_benefit"):
            candidates_from_ui["df_benefit"] = getattr(main_window, "df_benefit")

        if hasattr(main_window, "schedule_df"):
            candidates_from_ui["schedule_df"] = getattr(main_window, "schedule_df")

        if hasattr(main_window, "version_used"):
            # version_used 有可能是 dict，不是 DataFrame
            vu = getattr(main_window, "version_used")
            if isinstance(vu, pd.DataFrame):
                candidates_from_ui["version_used"] = vu
            elif isinstance(vu, dict):
                # 幫你轉小 DataFrame，之後測試 tooltip 或版本適用範圍很方便
                try:
                    vu_df = pd.DataFrame.from_dict(vu, orient="index")
                    candidates_from_ui["version_used"] = vu_df
                except Exception:
                    # 如果轉不了，就略過
                    pass

    # ------------------------------------------------------------
    # 2. 從 PI 抓幾支常用 tag (如果你願意這裡直接錄一份下來)
    #    這是選擇性動作。如果 pi_client=None 我們就跳過。
    # ------------------------------------------------------------
    if pi_client is not None:
        # 這裡請依你實際常看的 tag 列出清單
        # 例如鍋爐/發電機組/負載等資訊
        tags_to_capture = [
            "TG1:Power",
            "TG2:Power",
            # ... 想要錄哪幾支就加哪幾支
        ]

        # 假設未來你想錄最近一小時數據作為行為樣本
        end_t = datetime.datetime.now()
        start_t = end_t - datetime.timedelta(hours=1)

        for tag in tags_to_capture:
            try:
                series_or_df = pi_client.query_timeseries(
                    tag=tag,
                    start=start_t,
                    end=end_t,
                    interval="1min",   # 視你的 client 實作
                )
                candidates_from_ui[f"pi_{tag.replace(':','_')}"] = series_or_df
            except Exception as e:
                # 不因單一tag失敗就整個爆掉
                print(f"[export_debug_snapshot] WARN: failed to query {tag}: {e}")

    # ------------------------------------------------------------
    # 3. 寫檔
    #    每一個物件都用 save_sample_df() 存成一份獨立檔案
    # ------------------------------------------------------------
    for name, obj in candidates_from_ui.items():
        # 檔案名，例如 current -> current.parquet
        # 我們偏好 parquet，因為型別保留完整，之後測試最穩
        out_path = os.path.join(snapshot_dir, f"{name}.parquet")

        # 確保資料夾存在
        _ensure_dir(out_path)

        try:
            save_sample_df(
                obj,
                out_path=out_path,
                fmt=None,              # 讓 save_sample_df 自己依副檔名判斷 parquet
                max_rows=max_rows,
                round_ts=round_ts,
                columns=None,          # 你也可以指定只留必要欄位
                name=name,             # 如果是 Series，會拿來當欄名
            )
            print(f"[export_debug_snapshot] saved {name} -> {out_path}")
        except Exception as e:
            print(f"[export_debug_snapshot] ERROR saving {name}: {e}")

    print(f"[export_debug_snapshot] DONE. Snapshot dir: {snapshot_dir}")
    return snapshot_dir
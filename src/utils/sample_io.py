# src/utils/sample_io.py
import os
import pandas as pd

def _coerce_to_dataframe(obj, name: str = None):
    """
    把 Series 或 DataFrame 統一轉成 DataFrame，並標記這是不是原本的 Series。
    回傳 (df, is_series, series_name)
    """
    if isinstance(obj, pd.Series):
        df = obj.to_frame(name=name or obj.name or "value").reset_index()
        # 盡量猜 index 是時間戳，把它叫 timestamp
        if "index" in df.columns:
            df = df.rename(columns={"index": "timestamp"})
        is_series = True
        series_name = obj.name or name or "value"
    elif isinstance(obj, pd.DataFrame):
        df = obj.copy()
        is_series = False
        series_name = None
    else:
        raise TypeError(f"Unsupported type {type(obj)}; expected Series or DataFrame.")
    return df, is_series, series_name


def _infer_fmt_from_path(out_path: str) -> str:
    """
    根據副檔名推斷要存 parquet 還是 csv。
    """
    lower = out_path.lower()
    if lower.endswith(".parquet") or lower.endswith(".pq") or lower.endswith(".parq"):
        return "parquet"
    if lower.endswith(".csv"):
        return "csv"
    # 預設 parquet，因為它保留型別比較完整
    return "parquet"


def save_sample_df(
    obj,
    out_path: str,
    fmt: str = None,
    max_rows: int = 1000,
    round_ts: str = None,
    columns=None,
    name: str = None,
):
    """
    將 DataFrame 或 Series 儲存為最小可覆現樣本。

    參數說明：
    - obj: pd.DataFrame 或 pd.Series
    - out_path: 要輸出的檔案路徑 (可用 .parquet 或 .csv)
    - fmt: "parquet" / "csv" / None
           如果 None 則會依照 out_path 副檔名自動判斷
    - max_rows: 最多保留多少列 (避免整包超大資料塞進 repo)
    - round_ts: 將所有 datetime 欄位往下取到特定粒度，如 "15min"
                可以保護隱私 + 減少雜訊
    - columns: 只保留特定欄位的 list
    - name: 如果 obj 是 Series，就用這個名字當欄位名
    """
    df, is_series, series_name = _coerce_to_dataframe(obj, name=name)

    # 標記來源類型，方便之後 load 還原成 Series
    df["__is_series__"] = is_series
    df["__series_name__"] = series_name if is_series else ""

    # 篩欄位
    if columns is not None:
        keep_cols = [c for c in columns if c in df.columns]
        # 確保 metadata 欄位也在
        for extra in ["__is_series__","__series_name__"]:
            if extra not in keep_cols and extra in df.columns:
                keep_cols.append(extra)
        df = df.loc[:, keep_cols]

    # 處理時間欄位的 round (floor)
    if round_ts:
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df[col] = df[col].dt.floor(round_ts)

    # 刪減列數
    if max_rows and len(df) > max_rows:
        df = df.iloc[:max_rows]

    # 確保資料夾存在
    folder = os.path.dirname(out_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    # 判斷輸出格式
    use_fmt = fmt or _infer_fmt_from_path(out_path)

    if use_fmt == "parquet":
        df.to_parquet(out_path, index=False)
    elif use_fmt == "csv":
        df.to_csv(out_path, index=False)
    else:
        raise ValueError("fmt must be 'parquet' or 'csv'")


def load_sample_df(path: str):
    """
    根據副檔名讀 parquet / csv。
    如果檔案來自 Series，會自動轉回 Series。
    """
    lower = path.lower()
    if lower.endswith(".parquet") or lower.endswith(".pq") or lower.endswith(".parq"):
        df = pd.read_parquet(path)
    elif lower.endswith(".csv"):
        # parse_dates=True 是一個寬鬆策略，讓 timestamp 有機會自動變 datetime
        df = pd.read_csv(path, parse_dates=True)
        # 我們也可以再試著把 'timestamp' 欄轉成 datetime
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="ignore")
    else:
        raise ValueError("unsupported extension; use .parquet or .csv")

    # 嘗試還原成 Series
    if "__is_series__" in df.columns and df["__is_series__"].iloc[0]:
        # 找出資料欄（排除 metadata 欄位）
        value_cols = [c for c in df.columns if c not in ["timestamp","__is_series__","__series_name__"]]
        if len(value_cols) != 1:
            # 如果無法唯一決定哪一欄才是value，那就回傳 DataFrame，不強制縮成Series
            return df

        value_col = value_cols[0]

        if "timestamp" in df.columns:
            s = pd.Series(
                df[value_col].values,
                index=pd.to_datetime(df["timestamp"]),
                name=df["__series_name__"].iloc[0] or value_col,
            )
        else:
            s = pd.Series(
                df[value_col].values,
                name=df["__series_name__"].iloc[0] or value_col,
            )
        return s

    # 否則維持 DataFrame
    return df
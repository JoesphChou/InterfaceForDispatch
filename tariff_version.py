from typing import Union
from datetime import datetime
import pandas as pd

def find_tariff_version_range(columns, target_date):
    """
    給定一系列 datetime 欄位與查詢日期，回傳該日期適用的版本區間。

    - 若 target_date 落在版本中，回傳 (start_date, end_date)
    - 若為最新版本，end_date 為 None
    - 若早於所有版本，start_date 為 None，end_date 為最早版本

    參數：
    - columns: 可 iterable 的 datetime 欄位（如 df.columns 或 row.values）
    - target_date: datetime.date 型別

    回傳：
    - (start_date, end_date)：皆為 date 型別 或 None
    """
    columns = sorted([c for c in columns if pd.notna(c)], reverse=True)
    for i, col_date in enumerate(columns):
        if col_date.date() <= target_date:
            start_date = col_date.date()
            end_date = columns[i - 1].date() if i > 0 else None
            return start_date, end_date
    return None, columns[-1].date()  # target_date 太早，尚未適用任何版本

def get_current_rate_type_v6(
    raw_df: pd.DataFrame,
    holiday_dates: list,
    df_tariff: pd.DataFrame,
    target_datetime: Union[datetime, None] = None
) -> dict:
    """
    綜合參數，根據 time of use 資料 + 電價表 df_tariff，
    回傳目前時間適用的電價分類、版本、生效欄位與單價金額。

    參數:
    - raw_df: 從 time of use sheet 讀取的 DataFrame（未跳列）
    - holiday_dates: 國定假日清單（list of datetime.date）
    - df_tariff: 各分類歷史電價表，index 為分類名稱，columns 為各調整日期（字串或 datetime）
    - target_datetime: 欲查詢的時間（預設為現在）

    回傳:
    - dict，包含 rate_code、rate_label、unit_price、version_date 等資訊
    """

    def extract_version_range_from_row(version_row, target_date):
        """從變更日期 row 中推算出 target_date 所屬版本起迄"""
        columns = version_row.index
        values = version_row.values
        valid_versions = [(col, val) for col, val in zip(columns, values) if pd.notna(val)]
        valid_versions = sorted(valid_versions, key=lambda x: x[1], reverse=True)

        for i, (col, ver_date) in enumerate(valid_versions):
            if isinstance(ver_date, (datetime, pd.Timestamp)) and ver_date.date() <= target_date:
                start = ver_date.date()
                end = valid_versions[i - 1][1].date() if i > 0 else None
                return pd.Timestamp(start), end
        return None, valid_versions[-1][1].date() if valid_versions else (None, None)

    def format_range(start, end):
        if start and end:
            return f"{start.strftime('%Y/%m/%d')} ~ {end.strftime('%Y/%m/%d')}"
        elif start and not end:
            return f"{start.strftime('%Y/%m/%d')} ~（目前適用）"
        elif not start and end:
            return f"（最早版本）~ {end.strftime('%Y/%m/%d')}"
        else:
            return "（無有效版本）"

    if target_datetime is None:
        target_datetime = datetime.now()

    today = target_datetime.date()
    time = target_datetime.time()
    hour = time.hour
    minute = time.minute
    weekday = target_datetime.weekday()
    month_day = (today.month, today.day)

    # --- 將售電區塊獨立出來 ---
    try:
        df_sale = df_tariff.loc[["售電變更日期", "非離峰", "離峰"]]
        df_tariff = df_tariff.drop(["售電變更日期", "非離峰", "離峰"], errors="ignore")
    except Exception as e:
        return {"error": f"售電區塊分離錯誤: {e}"}

    # 1 購電版本日期
    if "購電變更日期" not in df_tariff.index:
        return {"error": "缺少『購電變更日期』欄位於 df_tariff"}
    purchase_ver_start, purchase_ver_end = extract_version_range_from_row(df_tariff.loc["購電變更日期"], today)

    # 2 售電版本日期
    if "售電變更日期" not in df_sale.index:
        return {"error": "缺少『售電變更日期』欄位於 df_tariff"}
    sale_ver_start, sale_ver_end = extract_version_range_from_row(df_sale.loc["售電變更日期"], today)

    # 2. 擷取欄位分類與夏季區間代碼 A/B
    type_row = raw_df.iloc[1, 1:7]
    ab_row = raw_df.iloc[2, 1:7]

    # 3. 判斷夏季 or 非夏季（A/B定義）
    summer_range_A = ((5, 16), (10, 15))
    summer_range_B = ((6, 1), (9, 30))

    def in_range(md, start, end):
        return (md >= start) and (md <= end)

    season_code = ab_row.iloc[0]
    if season_code == 'A':
        is_summer = in_range(month_day, *summer_range_A)
    elif season_code == 'B':
        is_summer = in_range(month_day, *summer_range_B)
    else:
        return {"error": f"不明的夏季區間定義：{season_code}"}

    # 4. 判斷是否是假日／週末
    is_weekend = weekday >= 5
    is_holiday = today in holiday_dates
    is_special_day = is_weekend or is_holiday

    # 5. 決定對應分類欄位名稱（如：夏季、夏週六...）
    if is_summer:
        if is_special_day:
            label_match = "夏週日,離峰日" if weekday == 6 or is_holiday else "夏週六"
        else:
            label_match = "夏季"
    else:
        if is_special_day:
            label_match = "非週日,離峰日" if weekday == 6 or is_holiday else "非夏季週六"
        else:
            label_match = "非夏季"

    # 6. 尋找分類欄位在 raw_df 中的對應欄
    matched_col_index = None
    for i, label in enumerate(type_row):
        if isinstance(label, str) and label.strip() == label_match:
            matched_col_index = i + 1
            break
    if matched_col_index is None:
        return {"error": f"找不到對應的電價欄位類別：{label_match}"}

    # 7. 決定要抓的 row（每半小時為一列）
    row_offset = hour * 2 + (1 if minute >= 30 else 0)
    excel_row = 3 + row_offset

    try:
        rate_code = int(raw_df.iloc[excel_row, matched_col_index])
    except Exception:
        return {"error": f"無法讀取 {target_datetime.strftime('%H:%M')} 的電價資料（row {excel_row}）"}

    # 8. 電價分類對應表
    rate_label_map = {
        1: "夏尖峰", 2: "夏半尖峰", 3: "夏離峰",
        4: "夏週六半", 5: "非夏半尖峰", 6: "非夏離峰", 7: "非夏週六半"
    }
    rate_label = rate_label_map.get(rate_code, "未知分類")

    # 9. 從 df_tariff 中找出對應購電單價
    unit_price = None
    if rate_label != "未知分類" and purchase_ver_start:
        try:
            version_row = df_tariff.loc['購電變更日期']
            col = version_row[version_row == pd.Timestamp(purchase_ver_start)].index[0]
            unit_price = df_tariff.loc[rate_label, col]
        except Exception:
            unit_price = None

    # --- 售電分類與價格 ---
    def classify_sale_type(label: str) -> str:
        return "離峰" if "離峰" in label else "非離峰"

    sale_type = classify_sale_type(rate_label)
    sale_price = None
    try:
        if sale_ver_start:
            version_row = df_sale.loc["售電變更日期"]
            col = version_row[version_row == pd.Timestamp(sale_ver_start)].index[0]
            sale_price = df_sale.loc[sale_type, col]
    except Exception:
        sale_price = None

    # 10. 整合結果
    return {
        "rate_code": rate_code,
        "rate_label": rate_label,
        "season": "夏季" if is_summer else "非夏季",
        "is_special": is_special_day,
        "hour": hour,
        "label_matched": label_match,
        "column_index": matched_col_index,
        "unit_price": unit_price,
        "purchase_ver_start": purchase_ver_start,
        "purchase_ver_end": purchase_ver_end,
        "sale_type": sale_type,
        "sale_price": sale_price,
        "sale_ver_start": sale_ver_start,
        "sale_ver_end": sale_ver_end,
        "purchase_range_text": format_range(purchase_ver_start, purchase_ver_end),
        "sale_range_text": format_range(sale_ver_start, sale_ver_end)
    }

def get_ng_generation_cost_v2(
    df_ng_history: pd.DataFrame,
    target_datetime: Union[datetime, None] = None
) -> dict:
    """
    根據 df_ng_history 各區塊資料對應的變更日期，
    自動比對 target_datetime 對應的 NG 成本計算結果（含 TG 維運成本、碳費）。

    資料區塊對應邏輯：
    - NG 價格欄位：由「NG牌價變更日期」那一列對應（如 NG 牌價 / NG 牌價(立方米)）
    - 熱值欄位：由「熱值變更日期」那一列對應（如 NG 熱值 / 蒸氣轉換電力）
    - TG 維運成本欄位：由「維運成本變更日期」那一列對應
    - 碳費欄位：由「碳費變更日期」那一列對應

    計算：
    - 可轉換電力 = NG 熱值 ÷ 蒸氣轉換電力
    - 天然氣發電成本 = NG 牌價 ÷ 可轉換電力
    """
    def extract_version_range_from_row(version_row, target_date):
        columns = version_row.index
        values = version_row.values
        valid_versions = [(col, val) for col, val in zip(columns, values) if pd.notna(val)]
        valid_versions = sorted(valid_versions, key=lambda x: x[1], reverse=True)

        for i, (col, ver_date) in enumerate(valid_versions):
            if isinstance(ver_date, (datetime, pd.Timestamp)) and ver_date.date() <= target_date:
                start = ver_date.date()
                end = valid_versions[i - 1][1].date() if i > 0 else None
                return col, start, end
        return valid_versions[-1][0], None, valid_versions[-1][1].date() if valid_versions else (None, None, None)

    def format_range(start, end):
        if start and end:
            return f"{start.strftime('%Y/%m/%d')} ~ {end.strftime('%Y/%m/%d')}"
        elif start and not end:
            return f"{start.strftime('%Y/%m/%d')} ~（目前適用）"
        elif not start and end:
            return f"（最早版本）~ {end.strftime('%Y/%m/%d')}"
        else:
            return "（無有效版本）"

    if target_datetime is None:
        target_datetime = datetime.now()

    target_date = target_datetime.date()
    df = df_ng_history.copy()

    # 欄位類別對應
    ng_price_fields = ["NG 牌價", "NG 牌價(立方米)"]
    heat_fields = ["NG 熱值", "蒸氣轉換電力"]
    maintain_field = "TG 維運成本"
    carbon_field = "碳費"

    # 取得對應欄位與版本資訊
    ng_col, ng_ver_start, ng_ver_end = extract_version_range_from_row(df.loc["NG牌價變更日期"], target_date)
    heat_col, heat_ver_start, heat_ver_end = extract_version_range_from_row(df.loc["熱值變更日期"], target_date)
    maintain_col, tg_ver_start, tg_ver_end = extract_version_range_from_row(df.loc["維運成本變更日期"], target_date)
    carbon_col, car_ver_start, car_ver_end = extract_version_range_from_row(df.loc["碳費變更日期"], target_date)

    # 取得 NG 價格
    try:
        ng_price = next(df.at[field, ng_col] for field in ng_price_fields if field in df.index and pd.notna(df.at[field, ng_col]))
    except StopIteration:
        ng_price = None

    try:
        ng_heat = df.at["NG 熱值", heat_col]
        steam_power = df.at["蒸氣轉換電力", heat_col]
    except Exception:
        ng_heat, steam_power = None, None

    try:
        tg_cost = df.at[maintain_field, maintain_col]
    except Exception:
        tg_cost = None

    try:
        carbon_cost = df.at[carbon_field, carbon_col]
    except Exception:
        carbon_cost = None

    if not all([ng_price, ng_heat, steam_power]):
        missing = [name for name, val in zip(["ng_price", "ng_heat", "steam_power", "carbon_cost"], [ng_price, ng_heat, steam_power]) if not val]
        return {"error": f"缺少欄位：{', '.join(missing)}"}

    try:
        convertible_power = ng_heat / steam_power
        ng_cost = ng_price / convertible_power
    except Exception as e:
        return {"error": f"計算錯誤: {e}"}

    return {
        "target_date": target_date,
        "ng_price": ng_price,
        "ng_price_ver_start": ng_ver_start,
        "ng_price_ver_end": ng_ver_end,
        "ng_price_range_text": format_range(ng_ver_start, ng_ver_end),
        "ng_heat": ng_heat,
        "steam_power": steam_power,
        "heat_ver_start": heat_ver_start,
        "heat_ver_end": heat_ver_end,
        "heat_range_text": format_range(heat_ver_start, heat_ver_end),
        "convertible_power": convertible_power,
        "ng_cost": ng_cost,
        "tg_maintain_cost": tg_cost,
        "tg_ver_start": tg_ver_start,
        "tg_ver_end": tg_ver_end,
        "tg_range_text": format_range(tg_ver_start, tg_ver_end),
        "carbon_cost": carbon_cost,
        "car_ver_start": car_ver_start,
        "car_ver_end": car_ver_end,
        "car_range_text":format_range(car_ver_start, car_ver_end),
        "formula": f"{ng_price} / ({ng_heat} ÷ {steam_power})"
    }

def get_ng_generation_cost(
    df_ng_history: pd.DataFrame,
    target_datetime: Union[datetime, None] = None
) -> dict:
    """
    根據 df_ng_history 各區塊資料對應的變更日期，
    自動比對 target_datetime 對應的 NG 成本計算結果（含 TG 維運成本）。

    資料區塊對應邏輯：
    - NG 價格欄位：由「NG牌價變更日期」那一列對應（如 NG 牌價 / NG 牌價(立方米)）
    - 熱值欄位：由「熱值變更日期」那一列對應（如 NG 熱值 / 蒸氣轉換電力）
    - TG 維運成本欄位：由「維運成本變更日期」那一列對應

    計算：
    - 可轉換電力 = NG 熱值 ÷ 蒸氣轉換電力
    - 天然氣發電成本 = NG 牌價 ÷ 可轉換電力
    """

    if target_datetime is None:
        target_datetime = datetime.now()

    target_date = target_datetime.date()

    df = df_ng_history.copy()

    # 各欄位對應類別
    ng_price_fields = ["NG 牌價", "NG 牌價(立方米)"]
    heat_fields = ["NG 熱值", "蒸氣轉換電力"]
    maintain_field = "TG 維運成本"

    # 抓對應版本的欄位位置
    def get_applicable_col(version_row_label):
        if version_row_label not in df.index:
            return None
        version_row = df.loc[version_row_label]
        valid_dates = [(col, val.date()) for col, val in version_row.items() if isinstance(val, datetime)]
        valid_dates.sort(key=lambda x: x[1], reverse=True)
        for col, ver_date in valid_dates:
            if ver_date <= target_date:
                return col
        return None

    ng_col = get_applicable_col("NG牌價變更日期")
    heat_col = get_applicable_col("熱值變更日期")
    maintain_col = get_applicable_col("維運成本變更日期")

    # 擷取資料值
    try:    # ** 如果"NG 牌價"沒有值，就抓"NG 牌價(立方米)) **
        ng_price = next(df.at[field, ng_col] for field in ng_price_fields if field in df.index and pd.notna(df.at[field, ng_col]))
    except StopIteration:
        ng_price = None

    try:
        ng_heat = df.at["NG 熱值", heat_col]
        steam_power = df.at["蒸氣轉換電力", heat_col]
    except Exception:
        ng_heat, steam_power = None, None

    try:
        tg_cost = df.at[maintain_field, maintain_col]
    except Exception:
        tg_cost = None

    # 檢查完整性與運算 (只要其中一個是空的，就回傳資料不足)
    if not all([ng_price, ng_heat, steam_power]):
        missing = [name for name, val in zip(["ng_price", "ng_heat", "steam_power"], [ng_price, ng_heat, steam_power])
                   if not val]
        if missing:
            return {"error": f"缺少欄位：{', '.join(missing)}"}

    try:
        convertible_power = ng_heat / steam_power
        ng_cost = ng_price / convertible_power
    except Exception as e:
        return {"error": f"計算錯誤: {e}"}

    # 結果回傳
    return {
        "target_date": target_date,
        "ng_price": ng_price,
        "ng_price_ver": df.at["NG牌價變更日期", ng_col].date() if pd.notna(df.at["NG牌價變更日期", ng_col]) else None,
        "ng_heat": ng_heat,
        "steam_power": steam_power,
        "heat_ver": df.at["熱值變更日期", heat_col].date() if pd.notna(df.at["熱值變更日期", heat_col]) else None,
        "convertible_power": convertible_power,
        "ng_cost": ng_cost,
        "tg_maintain_cost": tg_cost,
        "tg_ver": df.at["維運成本變更日期", maintain_col].date() if pd.notna(df.at["維運成本變更日期", maintain_col]) else None,
        "formula": f"{ng_price} / ({ng_heat} ÷ {steam_power})"
    }
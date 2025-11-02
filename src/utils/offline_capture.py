import os
import datetime
import pandas as pd
from src.utils.sample_io import save_sample_df

def capture_offline_bundle(main_window, pi_client, tag_list=None, out_root="offline_bundles"):
    """
    在有 PI 的環境執行。
    目的：把 PI 時序資料 + 目前主畫面計算結果，一起打包成離線用樣本。

    main_window: 目前執行中的 MyMainWindow (可以拿到 UI 內的資料表/計算結果)
    pi_client:   真正可以 query PI 的 client (PIClient)
    out_root:    儲存的根資料夾，例如 "offline_bundles"

    return: 實際輸出的資料夾路徑 (string)
    """

    ts_label = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    bundle_dir = os.path.join(out_root, f"offline_bundle_{ts_label}")
    os.makedirs(bundle_dir, exist_ok=True)

    # 1. 抓 PI tags（你決定哪些是「啟動程式最少需要」）
    #    例如你在 dashboard_value() 會查的幾個重點指標
    stack_chart_tags_list = [
        'W511_PH/33KV/2H_120/P', 'W511_PH/33KV/2H_220/P', 'W511_BOP/33KV/5H_120/P', 'W511_BOP/33KV/5H_220/P',
        'W511_MS2/33KV/1H_120/P', 'W511_MS2/33KV/1H_220/P', 'W511_MS2/33KV/1H_320/P', 'W511_MS2/33KV/1H_420/P',
        'W511_CW/33KV/4H_120/P', 'W511_CW/33KV/4H_220/P', 'W511_CW/11.5KV/4KA1_2_18/P', 'W511_BOP/11.5KV/5KB1_2_19/P',
        'W512_FX-201.PV', 'W512_FT-204.PV', 'W512_FT-203.PV', 'W512_FT-722.PV', 'W512_FT-739.PV', 'W512_FT-723.PV',
        'W512_FT-740.PV', 'W512_FT-733.PV', 'W512_FT-776.PV', 'W512_FT-734.PV', 'W512_FT-777.PV', 'W512_FT-213.PV',
        'W512_FT-214.PV', 'W512_FT-222.PV', 'W512_FT-223.PV', 'W512_FT-123.PV', 'W512_FT-125.PV',  'W512_FT-124.PV',
        'W512_FT-126.PV', 'W512_FT-146.PV', 'W512_FT-148.PV', 'W512_FT-147.PV', 'W512_FT-149.PV',
        ]
    et = pd.Timestamp.now().floor('S')
    st = et - pd.offsets.Minute(120)


    try:
        stack_chart_df = pi_client.query(st=st, et=et, tags=stack_chart_tags_list, summary='AVERAGE',
                                   interval='8s', fillna_method='ffill')

        out_path = os.path.join(bundle_dir, f"pi_stack_chart.csv")
        save_sample_df(
            stack_chart_df,
            out_path=out_path,
            fmt=None,          # 自動：副檔名 .parquet -> parquet
            max_rows=2000,     # 控資料量
        )
        print(f"[offline_capture] saved stack_chart_data -> {out_path}")
    except Exception as e:
        print(f"[offline_capture] WARN: cannot capture stack_chart_data: {e}")

    name_list = tag_list['tag_name'].dropna().tolist()
    try:
        current_series = pi_client.current_values(tags=name_list)
        out_path = os.path.join(bundle_dir, f"pi_dashboard_current.csv")
        save_sample_df(
            current_series,
            out_path=out_path,
            fmt=None,          # 自動：副檔名 .parquet -> parquet
            max_rows=2000,     # 控資料量
        )
        print(f"[offline_capture] saved pi_dashboard_current -> {out_path}")
    except Exception as e:
        print(f"[offline_capture] WARN: cannot capture pi_dashboard_current: {e}")

    # 2. 把主畫面上已經算好的結果也存起來
    #    例如 schedule_df, df_benefit, current, version_used 等
    ui_objects = {}

    if hasattr(main_window, "schedule_df"):
        ui_objects["schedule_df"] = main_window.schedule_df
    if hasattr(main_window, "df_benefit"):
        ui_objects["df_benefit"] = main_window.df_benefit
    if hasattr(main_window, "current"):
        ui_objects["current"] = main_window.current
    if hasattr(main_window, "version_used"):
        vu = main_window.version_used
        if isinstance(vu, dict):
            # dict 轉 DataFrame，方便離線重播 tooltip/版本資訊
            try:
                vu_df = pd.DataFrame.from_dict(vu, orient="index")
                ui_objects["version_used"] = vu_df
            except Exception:
                pass
        elif isinstance(vu, pd.DataFrame):
            ui_objects["version_used"] = vu

    for name, obj in ui_objects.items():
        out_path = os.path.join(bundle_dir, f"{name}.parquet")
        save_sample_df(
            obj,
            out_path=out_path,
            fmt=None,              # 自動 parquet
            max_rows=5000,         # 避免太肥
            round_ts="15min",      # 粗糙化時間戳，減少敏感細節
            name=name,
        )
        print(f"[offline_capture] saved {name} -> {out_path}")

    print(f"[offline_capture] DONE. bundle_dir = {bundle_dir}")
    return bundle_dir
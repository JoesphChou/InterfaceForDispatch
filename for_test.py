import pandas as pd
import urllib3
import re
from bs4 import BeautifulSoup

def scrapy_schedule():
    """
    解析 2138 和 2137 頁面，獲取不同製程 (EAF, LF1-1) 的排程
    - **確保 EAF & LF1-1 各自獨立去重複，不影響彼此**
    - **確保 LF1-1, LF1-2 past & future 不會消失**
    - **確保 2137 預計完成時間解析正常**
    解析 2138 和 2137 頁面，獲取不同製程 (EAF, LF1-1, LF1-2) 的排程，並去除重複排程 (透過爐號)
    """
    now = pd.Timestamp.now()
    today = now.normalize()
    today_str = today.strftime("%Y-%m-%d")  # 確保 'YYYY-MM-DD' 格式

    # **各製程獨立判斷重複**
    seen_furnace_ids = {
        "past": {"EAFA": set(), "EAFB": set(), "LF1-1": set(), "LF1-2": set()},
        "future": {"EAFA": set(), "EAFB": set(), "LF1-1": set(), "LF1-2": set()},
        "current": {"EAFA": set(), "EAFB": set(), "LF1-1": set(), "LF1-2": set()}
    }

    past_records, future_records, current_records = [], [], []

    ### **🔹 解析 2138 頁面 (排程)**
    quote_page = 'http://w3mes.dscsc.dragonsteel.com.tw/2138.aspx'
    http = urllib3.PoolManager()
    r = http.request('GET', quote_page)
    soup = BeautifulSoup(r.data, 'html.parser')

    contains = soup.find_all('area')
    schedule_data = []

    # **不同製程的時間匹配模式**
    time_patterns = {
        "EAFA": r"EAFA時間:\s*(\d{2}:\d{2}:\d{2})\s*~\s*(\d{2}:\d{2}:\d{2})",
        "EAFB": r"EAFB時間:\s*(\d{2}:\d{2}:\d{2})\s*~\s*(\d{2}:\d{2}:\d{2})",
        "LF1-1": r"LF1-1時間:\s*(\d{2}:\d{2}:\d{2})\s*~\s*(\d{2}:\d{2}:\d{2})",
        "LF1-2": r"LF1-2時間:\s*(\d{2}:\d{2}:\d{2})\s*~\s*(\d{2}:\d{2}:\d{2})"
    }

    for contain in contains:
        title_text = contain.get('title')

        # **取得爐號**
        furnace_match = re.search(r"爐號[＝>:\s]*([A-Za-z0-9]+)", title_text)
        furnace_id = furnace_match.group(1) if furnace_match else "未知"

        # **解析座標**
        coords = re.findall(r"\d+", contain.get('coords'))
        if len(coords) < 2:
            continue
        x_coord = int(coords[0])  # X 軸代表時間
        y_coord = int(coords[1])  # Y 軸代表排程分類

        # **根據 Y 座標範圍判斷製程類型**
        if 179 <= y_coord <= 197:
            process_type = "EAFA"
        elif 217 <= y_coord <= 235:
            process_type = "EAFB"
        elif 250 <= y_coord <= 268:
            process_type = "LF1-1"
        elif 286 <= y_coord <= 304:
            process_type = "LF1-2"
        else:
            continue  # 若不在任何範圍內則跳過

        # **檢查 title 是否包含對應製程名稱**
        if process_type not in title_text:
            continue  # **如果 title 不包含該製程名稱，則跳過解析**

        # **解析開始與結束時間**
        time_match = re.search(time_patterns[process_type], title_text)
        if not time_match:
            # print(f"⚠️ 無法解析 {process_type} 時間: {title_text}")
            continue  # 如果匹配失敗，跳過該排程

        start_time = time_match.group(1)
        end_time = time_match.group(2)

        # **轉換時間格式**
        start = pd.to_datetime(f"{today} {start_time}")
        end = pd.to_datetime(f"{today} {end_time}")

        # **存入數據，稍後進行 X 軸排序**
        schedule_data.append((x_coord, start, end, furnace_id, process_type))

    # 建立 sort_group 欄位：將 EAFA、EAFB 合併為 EAF，其它維持原樣
    def get_sort_group(process_type):
        if process_type in ["EAFA", "EAFB"]:
            return "EAF"
        return process_type

    # BUG 待解。如果X軸都在118，就必需要用起始時間來決定先後關係，不然可能會造成第2以(含)之後的排程都+1天.
    # 建立排序用資料
    schedule_data_with_group = [
        (x_coord, start, end, furnace_id, process_type, get_sort_group(process_type))
        for (x_coord, start, end, furnace_id, process_type) in schedule_data
    ]

    # 根據 sort_group 與 x_coord 排序，如果x_coord相同時，則用起始時間排序
    schedule_data_with_group.sort(key=lambda x: (x[5], x[0], x[1]))

    # 移除排序欄位後，回復為原本格式
    schedule_data = [(x[0], x[1], x[2], x[3], x[4]) for x in schedule_data_with_group]

    # **根據 X 軸進行排序**
    #schedule_data.sort(key=lambda x: (x[4], x[0]))  # 先按 process_type 再按 X 座標 排序

    # **去除重複排程 (相同的爐號id)**
    filtered_schedule = []
    for i in range(len(schedule_data)):
        if i > 0:
            curr_x, curr_start, curr_end, curr_furnace, curr_process = schedule_data[i]
            # **讀取已相同製程已存在的爐號id**
            furnace_list = [e[3] for e in filtered_schedule if e[4]==curr_process]
            # **如果目前處理的爐號id 己存在，則不加入 filtered_schedule
            if curr_furnace in furnace_list:
                #print(f"⚠️ 重複排程移除: {curr_process} {curr_start} ~ {curr_end} (X={curr_x})")
                continue  # **跳過這筆排程，不加入 filtered_schedule**

        filtered_schedule.append(schedule_data[i])

    # 加入檢查排程時間錯亂及跨天的邏輯
    for i in range(len(filtered_schedule)):
        curr_x, curr_start, curr_end, curr_furnace, curr_process = filtered_schedule[i]

        # 如果排程的結束時間比開始時間早，表示跨天，結束時間需加一天
        if curr_end < curr_start:
            curr_end += pd.Timedelta(days=1)

        # 如果目前系統時間在00:00~08:00, 且距離排程開始生產的時間，如果超過10小時以上，則判斷為前一天已生產完的排程
        now = pd.Timestamp.now()
        if now < (pd.Timestamp.today().normalize() + pd.offsets.Hour(8)):
            if (abs(now - curr_start)) > pd.Timedelta(hours=10):
                curr_start -= pd.Timedelta(days=1)
                curr_end -= pd.Timedelta(days=1)
        elif i == 0:    # 如果前面都沒有排程，遇到第一筆資料的日期超過現在10小時，則判斷為跨日的排程。
            if abs(now - curr_start) > pd.Timedelta(hours=10):
                curr_start += pd.Timedelta(days=1)
                curr_end += pd.Timedelta(days=1)

        # 如果同一製程有前一筆排程，且當前開始時間比前一排程開始時間還早，則跨天，需加一天 (EAFA 和 EAFB 視為同一種製程)
        def unify_process(p):
            return "EAF" if p in ("EAFA", "EAFB") else p

        if i > 0:
            prev_x, prev_start, prev_end, prev_furnace, prev_process = filtered_schedule[i - 1]
            if unify_process(curr_process) == unify_process(prev_process) and curr_start < prev_start:
                curr_start += pd.Timedelta(days=1)
                curr_end += pd.Timedelta(days=1)

        # 更新 schedule_data 中的資料
        filtered_schedule[i] = (curr_x, curr_start, curr_end, curr_furnace, curr_process)

    ### **🔹 解析 2137 頁面 (獲取製程狀態)**
    quote_page_2137 = 'http://w3mes.dscsc.dragonsteel.com.tw/2137.aspx'
    r_2137 = http.request('GET', quote_page_2137)
    soup_2137 = BeautifulSoup(r_2137.data, 'html.parser')

    # **解析製程狀態**
    def get_status(soup, element_id):
        status_element = soup.find("span", {"id": element_id})
        return status_element.text.strip() if status_element else "未知"

    process_status_mapping = {
        "EAFA": get_status(soup_2137, "lbl_eafa_period"),
        "EAFB": get_status(soup_2137, "lbl_eafb_period"),
        "LF1-1": get_status(soup_2137, "lbl_lf11_period"),
        "LF1-2": get_status(soup_2137, "lbl_lf12_period"),
    }

    # **分類 past / current / future**
    for x_coord, start, end, furnace_id, process_type in filtered_schedule:
        if end < now:
            if furnace_id not in seen_furnace_ids["past"][process_type]:
                past_records.append([start, end, furnace_id, process_type])
                seen_furnace_ids["past"][process_type].add(furnace_id)
        elif start > now:
            if furnace_id not in seen_furnace_ids["future"][process_type]:
                future_records.append([start, end, furnace_id, process_type])
                seen_furnace_ids["future"][process_type].add(furnace_id)
        else:
            if furnace_id not in seen_furnace_ids["current"][process_type]:
                process_status = process_status_mapping.get(process_type, "未知")
                current_records.append([start, end, furnace_id, process_type, process_status])  # **確保 製程狀態 存在**
                seen_furnace_ids["current"][process_type].add(furnace_id)

    ### **🔹 轉換為 DataFrame**
    past_df = pd.DataFrame(past_records, columns=["開始時間", "結束時間", "爐號", "製程"])
    current_df = pd.DataFrame(current_records, columns=["開始時間", "結束時間", "爐號", "製程", "製程狀態"])
    future_df = pd.DataFrame(future_records, columns=["開始時間", "結束時間", "爐號", "製程"])

    return past_df, current_df, future_df

scrapy_schedule()

import pandas as pd
import urllib3
import re
from bs4 import BeautifulSoup

def scrapy_schedule():
    """
    爬取"製程管理資訊 2138"，解析電爐排程，並做以下處理：
    1. 只處理 title 含有 "EAF" 的排程
    2. 只讀取 Y 軸在 182~235 範圍內的排程
    3. 透過 X 軸排序，確保排程順序正確
    4. **檢查 start、end 是否跨天**
    5. **確保後面排程的時間不會比前面排程還早**
    6. **分類排程**
        - `past`: 過去排程 (`end < now`)
        - `current`: 正在生產 (`start <= now <= end`)
        - `future`: 未來排程 (`start > now`)
    爬取"製程管理資訊 2137"，正在生產中的電爐製程狀態，並把資訊加到current 裡面
    :return: past, current, future 排程列表
    """
    past = []
    future = []
    current = []
    now = pd.Timestamp.now()  # 當前時間（含日期與時間）
    today = now.normalize()  # 取得今日日期（只保留 YYYY-MM-DD）

    quote_page = 'http://w3mes.dscsc.dragonsteel.com.tw/2138.aspx'
    quote_page2 = 'http://w3mes.dscsc.dragonsteel.com.tw/2137.aspx'

    http = urllib3.PoolManager()
    r = http.request('GET', quote_page)
    r2 = http.request('GET', quote_page2)
    soup = BeautifulSoup(r.data, 'html.parser')
    soup2 = BeautifulSoup(r2.data, 'html.parser')

    contains = soup.find_all('area')  # 找出所有的 <area> tag
    schedule_data = []  # 存儲 (x座標, start_time, end_time, title)

    for contain in contains:
        title_text = contain.get('title')  # 取得 title 內容

        # **只處理包含 "EAF" 的標題**
        if 'EAF' not in title_text:
            continue

        # 判斷是 A爐 (EAFA) 還是 B爐(EAFB)
        furnace_type = None
        if 'EAFA' in title_text:
            furnace_type = 'A'
        elif 'EAFB' in title_text:
            furnace_type = 'B'
        else:
            continue    # 若無法判別爐別，跳過此排程

        # 取得座標
        coords = re.findall(r"\d+", contain.get('coords'))  # 解析座標
        if len(coords) < 2:
            continue  # 座標解析失敗則跳過

        x_coord = int(coords[0])  # X 座標（代表時間順序）
        y_coord = int(coords[1])  # Y 座標

        # **篩選 Y 軸在 182~235 的範圍**
        if not (182 < y_coord < 235):
            continue  # 不符合範圍，跳過此排程

        # 解析出 start 和 end 時間
        time_pattern = re.findall(r"(\d{2}:\d{2}:\d{2})", title_text)
        if len(time_pattern) < 2:
            continue  # 無法解析時間則跳過

        start_time = time_pattern[0]  # 取得開始時間 (未加日期)
        end_time = time_pattern[1]    # 取得結束時間 (未加日期)

        # 存入數據，稍後排序
        schedule_data.append((x_coord, start_time, end_time, title_text, furnace_type))

    # **根據 X 座標 (時間軸) 進行排序**
    schedule_data.sort(key=lambda x: x[0])  # 按 x 座標排序

    # **調整時間順序，確保排程不會比前一個早**
    adjusted_schedule = []
    prev_start_time = None  # 記錄前一個排程的開始時間

    for x_coord, start_time, end_time, title_text, furnace_type in schedule_data:
        # 先將時間轉換為當前日期的時間格式
        start = pd.to_datetime(f"{today} {start_time}")
        end = pd.to_datetime(f"{today} {end_time}")

        # **若 end < start，則 end 應跨天**
        if end < start:
            end += pd.Timedelta(days=1)

        # **檢查是否時間錯亂 (X 軸較後的排程卻比前一個排程早)**
        if prev_start_time and start < prev_start_time:
            # **如果新的 start 比前一個 start 還早，表示需要 +1 天**
            start += pd.Timedelta(days=1)
            end += pd.Timedelta(days=1)

        # 更新 prev_start_time
        prev_start_time = start

        # **加入調整後的排程**
        adjusted_schedule.append((start, end, furnace_type))

    # 嘗試根據 id 找出 A爐與 B爐的製程狀態
    a_furnace_status = soup2.find(id="lbl_eafa_period")
    b_furnace_status = soup2.find(id="lbl_eafb_period")

    # 取得文字內容
    a_furnace_status_text = a_furnace_status.get_text(strip=True) if a_furnace_status else "未找到"
    b_furnace_status_text = b_furnace_status.get_text(strip=True) if b_furnace_status else "未找到"

    # **分類排程**
    for start, end, furnace_type in adjusted_schedule:
        if end < now:
            past.append(pd.Series([start, end, furnace_type]))  # 過去的排程
        elif start > now:
            future.append(pd.Series([start, end, furnace_type]))  # 未來的排程
        else:   # 判斷由A或B爐生產，並讀取相對應的製程狀態
            if furnace_type == 'A':
                current.append(pd.Series([start, end, furnace_type, a_furnace_status_text]))
            elif furnace_type == 'B':
                current.append(pd.Series([start, end, furnace_type, b_furnace_status_text]))
            else:
                current.append(pd.Series([start, end, furnace_type, '未知']))
    return past, current, future

scrapy_schedule()

def a():
    # 解析 2137 HTML，提取 A爐和 B爐的製程狀態

    quote_page = 'http://w3mes.dscsc.dragonsteel.com.tw/2137.aspx'
    http = urllib3.PoolManager()
    r = http.request('GET', quote_page)
    soup_2137 = BeautifulSoup(r.data, 'html.parser')

    # 嘗試根據 id 找出 A爐與 B爐的製程狀態
    a_furnace_status = soup_2137.find(id="lbl_eafa_period")
    b_furnace_status = soup_2137.find(id="lbl_eafb_period")

    # 取得文字內容
    a_furnace_status_text = a_furnace_status.get_text(strip=True) if a_furnace_status else "未找到"
    b_furnace_status_text = b_furnace_status.get_text(strip=True) if b_furnace_status else "未找到"

    # 顯示 A爐與 B爐的製程狀態
    {"A爐": a_furnace_status_text, "B爐": b_furnace_status_text}
a()
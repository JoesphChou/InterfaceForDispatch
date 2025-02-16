# Form implementation generated from reading ui file 'for_test.py'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.

import time
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
import re

def timeit(func):
    def wrapper(*args, **kwargs):
        s = time.time()
        func(*args, **kwargs)
        print(func.__name__, 'total time', time.time()-s)
    return wrapper

@timeit
def test():
    print('test')
"""     naive local time, tz aware 轉換
print(pd.Timestamp.now())    # naive local time
print(pd.Timestamp.utcnow())  # tz aware UTC
print(pd.Timestamp.now(tz='Europe/Brussels'))   # tz aware local time
print(pd.Timestamp.now(tz='Europe/Brussels').tz_localize(None))  # naive local time
print(pd.Timestamp.now(tz='Europe/Brussels').tz_convert(None))  # naive UTC
print(pd.Timestamp.utcnow().tz_localize(None))  # naive UTC
print(pd.Timestamp.utcnow().tz_convert(None))  # naive UTC
"""

user='036303'
password='a58705113'

quote_page = 'http://w3mes.dscsc.dragonsteel.com.tw/2138.aspx'
quote_page1 = 'http://w3mes.dscsc.dragonsteel.com.tw/2137.aspx'
quote_page2 = 'http://prod.cdgs.com.tw/erp/wk/jsp/wkjjCDGSDRO.jsp'

headers = urllib3.make_headers(basic_auth=f'{user}:{password}')
headers1 = urllib3.util.make_headers(basic_auth=f'{user}:{password}')
headers2 = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Host": quote_page2,
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded",
    "Sec-GPC": 1,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
}


http= urllib3.PoolManager()

r = http.request('GET',quote_page)                 # 透過HTTP 請求從"製程管理資訊 2138"獲取網頁

soup = BeautifulSoup(r.data, 'html.parser')  # 用BS 的html.parer 解析該網頁
contains = soup.find_all('area')   # 尋找內容裡所有名稱叫做area 的tag (圖像地圖區域元素)

t_count = 0
f_count = 0
p_count = 0

past = list()
future = list()
# current = pd.Series()
print(soup)
for contain in contains:
    if 'EAF' in contain.get('title'):             # 找出含有EAF 的title
        coords = re.findall(r"\d+",contain.get('coords'))   # 提取出 title 的座標
        # \d+ 是一個正則表達示，意思是"一個或多個數字(0-9)
        print(coords)
        print(contain.get('title'))
        print('----------------')
        if (int(coords[1]) > 182) & (int(coords[1]) < 235): # 利用 title 左上角的y軸座標,判斷title 的內容要不要提取
            t_count = t_count + 1
            if '送電' in contain.get('title'):         # 已結束EAF 製程的部份(特徵為內容中有'送電'這個詞)
                p_count = p_count + 1
                pending_str = contain.get('title')    # 從contain 中獲取 title 的內容
                # result = pending_str[pending_str.find(':') + 2: pending_str.find(':') + 15]
                start = pd.to_datetime(pending_str[pending_str.find(':') + 2: pending_str.find(':') + 7])
                end = pd.to_datetime(pending_str[pending_str.find(':') + 10: pending_str.find(':') + 15])
                if start > end:         # 若end time 比star time早,確認是跨天,end time +1 day
                    end = end + pd.offsets.Day(1)
                result = pd.Series([start,end])
                past.append(result)

            if '時間' in contain.get('title'):         # 還未完成EAF 製程的部份 (特徵為第一個找到的'時間'這個詞)
                pending_str = contain.get('title')    # 從contain 中獲取 title 的內容
                start = pd.to_datetime(pending_str[pending_str.find(':') + 2: pending_str.find(':') + 10])
                end = pd.to_datetime(pending_str[pending_str.find(':') + 13: pending_str.find(':') + 21])
                if start > end:         # 若end time 比star time早, 確認是跨天 (end time +1 day)
                    end = end + pd.offsets.Day(1)
                #if (pd.Timestamp.now() > start) and (pd.Timestamp.now() > end):
                #    start = start + pd.offsets.Day(1)
                #    end = end + pd.offsets.Day(1)
                if start > pd.Timestamp.now():        # 用來過濾掉非未來排程的部份
                    f_count = f_count + 1
                    result = pd.Series([start,end])
                    future.append(result)
                elif (start < pd.Timestamp.now()) and (pd.Timestamp.now() < end):   # 正在執行的排程
                    current = pd.Series([start,end])
                else:
                    t_count = t_count - 1

print('Total: ', t_count)
print('Future:', f_count)
for period in future:
    print('%s ~ %s' % (period[0],period[1]))


print('Past:' , p_count)
for period in past:
    print('%s ~ %s' % (period[0],period[1]))

print('Current:')
print('%s ~ %s' % (current[0],current[1]))
"""
pending_str = contain.get('title')      # 從contain 中獲取 title 的內容
# 從待解析的文字中，獲取該EAF 的起始、結束時間，並轉成為datatime格式
start_t = datetime.strptime(pending_str[pending_str.find(':') + 2: pending_str.find(':') + 7] + ':00', '%H:%M:%S')
                                                                                          10
end_t = datetime.strptime(pending_str[pending_str.find(':') + 10: pending_str.find(':') + 15] + ':00', '%H:%M:%S')
                                                              12                          21 
        if '送電' in contain.get('title'):         # 已結束EAF 製程的部份
            p_count = p_count + 1
            pending_str = contain.get('title')  # 從contain 中獲取 title 的內容
            result = pending_str[pending_str.find(':') + 2: pending_str.find(':') + 15]
            past.append(result)
            
        
        if '時間' in contain.get('title'):         # 還未完成EAF 製程的部份
            f_count = f_count + 1
            pending_str = contain.get('title')  # 從contain 中獲取 title 的內容
            result = pending_str[pending_str.find(':') + 2: pending_str.find(':') + 21]
            future.append(result)                                                       
"""


"""
r1 = http.request('GET',quote_page1)        # 透過HTTP 請求從"製程管理資訊 2137"獲取網頁
soup = BeautifulSoup(r1.data,'html.parser')             # 用BS 的html.parer 解析該網頁
period_eafa = soup.find_all('span', id='lbl_eafa_period')[0].get_text()   # 尋找tag 為span, id為對應EAF A爐的內容
period_eafb = soup.find_all('span', id='lbl_eafb_period')[0].get_text()   # 尋找tag 為span, id為對應EAF B爐的內容

print(period_eafa)
print('電爐A: ', period_eafa)
print(period_eafb)
print('電爐B: ', period_eafb)
#print(soup)
"""

import PIconnect as Pi
from PyQt6 import QtCore, QtWidgets, QtGui
from PyQt6.QtWidgets import QTableWidgetItem
from PyQt6.QtGui import QColor, QBrush
from UI import Ui_Form
import sys, re
import pandas as pd
import time, math
import urllib3
from bs4 import BeautifulSoup

def timeit(func):
    print('接到 func', func.__name__)
    def wrapper(*args, **kwargs):
        print('幫忙代入 args', args)
        print('幫忙代入 kwargs', kwargs)
        s = time.time()
        result = func(*args, **kwargs)
        print(func.__name__, 'total time', time.time()-s)
        return result
    return wrapper

def query_pi(st, et, tags, extract_type, time_offset = 0):
    """
        1. 從 PI 取出的 timestamp 時區改成 GMT+8   123
        2. 用 PI.PIServer().search 找出tag 對應的PIPoint，回傳的結果是list 型態。
           將該結果從list 提出，並新增到points 的list 中。
        3. 針對每一個PIPoint 透過 summaries 的方法，依extract_type 內容，決定特定區間取出值為何種形式。
           此方法回傳的資料為DataFrame 型態
        4. 將每筆DataFrame 存成list 之前，將資料型態從Object -> float，若有資料中有文字無法換的，則用NaN 缺失值取代。
           這邊使用的column名稱 ('RANGE')，必須視依不同的extract type 進行調整。
        5. 將list 中所有的 DataFrame 合併為一組新的 DataFrame 資料
        6. 把原本用來做index 的時間，將時區從tz aware 改為 native，並加入與OSAKI 時間差參數進行調整。
    :param st:  區間起始點的日期、時間
    :param et:  區間結束點的日期、時間
    :param tags:  list。 要查調的所有 tag
    :param extract_type: 預設為 16。16 -> PI.PIConsts.SummaryType.RANGE
                                   8 -> PI.PIConsts.SummaryType.MAXIMUM
                                   4 -> PI.PIConstsSummaryType.MINIMUM
    :param time_offset: 預設為 0。 用來近似 與 OSAKI 時間用的參數(秒數)
    :return: 將結果以 DataFrame 格式回傳。 shape(資料數量, tag數量)
    """
    st = st - pd.offsets.Second(time_offset)
    et = et - pd.offsets.Second(time_offset)
    Pi.PIConfig.DEFAULT_TIMEZONE = 'Asia/Taipei'        #1
    # summarytype = [16,8,4]
    with Pi.PIServer() as server:
        points = list()
        for tag_name in tags:
            points.append(server.search(tag_name)[0])   #2
        buffer = list()
        for x in range(len(points)):
            data = points[x].summaries(st, et, '15m', extract_type)                  # 3
            data['RANGE'] = pd.to_numeric(data['RANGE'], errors='coerce')            # 4
            buffer.append(data)
        raw_data = pd.concat([s for s in buffer], axis=1)                            # 5
        raw_data.set_index(raw_data.index.tz_localize(None)
                           + pd.offsets.Second(time_offset), inplace = True)         # 6
    return raw_data

def pre_check(pending_data, b=1, c='power'):
    """
    此函式用來判顯示在tree,table widget  的即時資料，是否有資料異常、設備沒有運轉或停機的狀況 (數值接近 0)
    :param c: 用來判斷是燃氣或電力的類別
    :param pending_data:要判斷的數值。
    :param b:若數值接近 0，預設回傳'停機'的述述。
    :return: 回傳值為文字型態。
    """
    describe = ['--', '停機', '資料異常','未使用','0 MW','未發電']
    if pd.isnull(pending_data):
        return describe[2]
    if pending_data > 0.1:
        if c == 'gas':
            return str(format(round(pending_data, 1),'.1f'))
            # return str(format(round(pending_data, 1), '.1f')) + ' Nm3/hr'
        elif c == 'h':
            return str(format(round(pending_data, 2), '.2f'))
        else:
            return str(format(round(pending_data, 2),'.2f')) + ' MW'
    else:
        return describe[b]

def pre_check2(pending_data, b=1):
    """
    此函式用來判顯示在tree,table widget  的 "歷史" 資料，是否有資料異常、設備沒有運轉或停機的狀況 (數值接近 0)
    :param b: 用來指定用那一個describe，預設為'停機'
    :param pending_data:
    :return:
    """
    describe = ['--', '停機', '資料異常', '未使用', '0 MW', '未發電']
    if pd.isnull(pending_data):
        return describe[2]
    if pending_data > 0.1:
        return str(format(round(pending_data, 2), '.2f'))
    else:
        return describe[b]

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

class MyMainForm(QtWidgets.QMainWindow, Ui_Form):

    def __init__(self):
        super(MyMainForm, self).__init__()
        self.setupUi(self)
        self.pushButton.clicked.connect(self.query_cbl)
        self.pushButton_2.clicked.connect(self.add_list_item)
        self.pushButton_3.clicked.connect(self.remove_list_item1)
        self.pushButton_4.clicked.connect(self.query_demand)
        self.dateEdit.setDate(QtCore.QDate().currentDate())
        self.dateEdit_2.setDate(QtCore.QDate().currentDate())
        self.spinBox.setValue(5)
        self.spinBox_2.setValue(4)
        self.listWidget.doubleClicked.connect(self.remove_list_item1)
        self.spinBox_2.valueChanged.connect(self.tz_changed)
        self.timeEdit.dateTimeChanged.connect(self.tz_changed)
        self.tableWidget_2.itemSelectionChanged.connect(self.handle_selection_changed)
        self.tag_list = pd.read_excel('.\parameter.xlsx', sheet_name=0)
        self.special_dates = pd.read_excel('.\parameter.xlsx', sheet_name=1)
        self.unit_prices = pd.read_excel('.\parameter.xlsx', sheet_name=2, index_col=0, skiprows=[4, 12, 16])
        self.define_cbl_date(pd.Timestamp.now().date())   # 初始化時，便立即找出預設的cbl參考日，並更新在list widget 裡
        self.tw1.itemExpanded.connect(self.tw1_expanded_event)
        self.tw1.itemCollapsed.connect(self.tw1_expanded_event)
        self.tw3.itemExpanded.connect(self.tw3_expanded_event)
        self.tw3.itemCollapsed.connect(self.tw3_expanded_event)
        self.checkBox.stateChanged.connect(self.check_box_event)
        self.checkBox_2.stateChanged.connect(self.check_box2_event)
        self.query_cbl()      # 查詢特定條件的 基準用電容量(CBL)
        self.query_demand()   # 查詢某一天每一週期的Demand
        self.tws_init()
        self.dashboard_value()

        # 使用QThread 的多執行緒，與自動更新選項動作綁定，執行自動更新current value
        self.thread_to_update = QtCore.QThread()
        self.thread_to_update.run = self.update_current_value
        self.thread_to_update.start()

        self.history_datas_of_groups = pd.DataFrame()  # 用來紀錄整天的各負載分類的週期平均值
        # ------- 關於比對歷史紀錄相關功能的監聽事件、初始狀況及執行設定等 ---------
        self.horizontalScrollBar.valueChanged.connect(self.confirm_value)
        self.dateEdit_3.dateChanged.connect(self.date_edit3_user_change)
        self.checkBox_2.setChecked(False)
        #-------- 各tree widgets、table widgets 的欄位寬、總寬、高等設定---------

    def tws_init(self):
        """
        1. 因為treeWidget 的item 文字對齊方式，不知道為何從ui.ui 轉成UI.py 時，預設值都跑掉，所以只能先暫時在這邊設置
        :return:
        """
        # 美化 tw1, tw2, tw3（QTreeWidget）
        self.beautify_avg_column(self.tw1, 2)
        self.beautify_avg_column(self.tw2, 2)
        self.beautify_avg_column(self.tw3, 2)

        # 初始化及美化 tableWidget_3（QTableWidget）
        self.initialize_tableWidget_3_colors()
        self.beautify_avg_column(self.tableWidget_3, 2)
        self.beautify_avg_column(self.tableWidget_4, 0)

        avg_column_width = 65
        self.tw1.setStyleSheet("QHeaderView::section{background:rgb(85, 181, 200);}")  # 設置表頭的背景顏色
        brush = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # brush 用來設定顏色種類
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)  # 設定顏色的分佈方式
        self.tw1.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw1.headerItem().setForeground(1, brush)
        self.tw1.headerItem().setForeground(2, brush)

        #scroller width 18, frame line width 1
        self.tw1.setColumnWidth(0, 175)  # 設定各column 的寬度
        self.tw1.setColumnWidth(1, 90)
        self.tw1.setColumnWidth(2, avg_column_width)
        tw1_width = self.tw1.columnWidth(0) + self.tw1.columnWidth(1) + self.tw1.columnWidth(2) + 20
        self.tw1.setFixedWidth(tw1_width)

        self.tw2.setStyleSheet("QHeaderView::section{background:rgb(85, 181, 200);}")  # 設置表頭的背景顏色
        self.tw2.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw2.headerItem().setForeground(1, brush)
        self.tw2.headerItem().setForeground(2, brush)
        self.tw2.setColumnWidth(0, 135)     # 設定各column 的寬度
        self.tw2.setColumnWidth(1, 90)
        self.tw2.setColumnWidth(2, avg_column_width)
        tw2_width = self.tw2.columnWidth(0) + self.tw2.columnWidth(1) + self.tw2.columnWidth(2)
        self.tw2.setFixedWidth(tw2_width)

        self.tw3.setStyleSheet("QHeaderView::section{background:rgb(100, 170, 90);}")  # 設置表頭的背景顏色
        brush = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # brush 用來設定顏色種類
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)  # 設定顏色的分佈方式
        self.tw3.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw3.headerItem().setForeground(1, brush)
        self.tw3.headerItem().setForeground(2, brush)
        self.tw3.setColumnWidth(0, 110)  # tw3 total width: 221
        self.tw3.setColumnWidth(1, 100)
        self.tw3.setColumnWidth(2, avg_column_width)
        self.tableWidget_3.setColumnWidth(0, 100)
        self.tableWidget_3.setColumnWidth(1, 100)
        self.tableWidget_3.setColumnWidth(2, avg_column_width)
        new_width = (self.tw3.columnWidth(0) + self.tw3.columnWidth(1) + self.tw3.columnWidth(2)+20)

        self.tableWidget_3.setFixedWidth(new_width)

        self.tableWidget_4.setRowCount(1)
        self.tableWidget_4.setColumnWidth(0, 160)
        self.tableWidget_4.setColumnWidth(1, 80)

        """
        1. 美化 tableWidget_4 的標題列 (Column Header)
        2. **確保 horizontal scroller 不會出現**
        3. **確保 vertical scroller 依需要出現**
        4. **調整 tableWidget_4 的總寬度與高度**
        """
        # 1️⃣ **美化標題列**
        header = self.tableWidget_4.horizontalHeader()

        # 設定標題列背景顏色 (淺藍色)
        self.tableWidget_4.setStyleSheet(
            "QHeaderView::section { background-color: #ADD8E6; color: #333333; font-weight: bold; font-family: '微軟正黑體'; }"
        )

        # 設定標題列對齊方式 (置中)
        header.setDefaultAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        # 2️⃣ **設定滾動條**
        self.tableWidget_4.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)  # 垂直滾動條根據需要顯示
        self.tableWidget_4.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)  # 隱藏水平滾動條

        # 3️⃣ **調整欄寬**
        col_1_width = 200  # EAF 排程時間欄位寬度
        col_2_width = 140  # 狀態欄位寬度
        vertical_scroller_width = 20  # 垂直滾動條寬度
        table_border_width = 2  # 表格邊框線寬

        self.tableWidget_4.setColumnWidth(0, col_1_width)
        self.tableWidget_4.setColumnWidth(1, col_2_width)

        # **確保水平滾動條不會出現，剛好填滿表格**
        total_table_width = col_1_width + col_2_width + vertical_scroller_width + table_border_width
        self.tableWidget_4.setMinimumWidth(total_table_width)
        self.tableWidget_4.setMaximumWidth(total_table_width)  # 固定寬度，防止變大

        # 4️⃣ **設定高度：最多顯示 4 筆排程**
        row_height = 35  # 每行高度
        max_rows = 4  # 最多顯示 4 行，其他的靠滾動條
        header_height = 30  # 標題列高度
        total_height = (row_height * max_rows) + header_height + 5  # 加 5 讓滾動條不擋住最後一行

        self.tableWidget_4.setMinimumHeight(total_height)
        self.tableWidget_4.setMaximumHeight(total_height)  # 固定高度，不會自動變大

        # 5️⃣ **調整行高**
        self.tableWidget_4.verticalHeader().setDefaultSectionSize(row_height)  # 預設行高 35px

        # 6️⃣ **設定不可編輯模式**
        self.tableWidget_4.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)  # 禁止編輯
        self.tableWidget_4.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)  # 選取整行

        # ---------------以下是針對每個treeWidget 設定文字對齊、顏色---------------
        brush2 = QtGui.QBrush(QtGui.QColor(180, 180, 180))  # brush2 用來設定設備群子項的即時量顏色
        brush2.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush3 = QtGui.QBrush(QtGui.QColor(0, 0, 255))  # brush3 用來各一級單位即時量的顏色
        brush3.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        # other -> W2
        self.tw1.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).setForeground(1, brush3)
        # other -> W2 -> 鼓風機
        self.tw1.topLevelItem(0).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
        self.tw1.topLevelItem(0).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(0).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(0).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(0).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(0).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(0).child(0).child(1).setForeground(1, brush2)
        self.tw1.topLevelItem(0).child(0).child(2).setForeground(1, brush2)
        # other -> W2 -> #1 燒結風車
        self.tw1.topLevelItem(0).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        # other -> W2 -> #2 燒結風車
        self.tw1.topLevelItem(0).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
        self.tw1.topLevelItem(0).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(2).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(2).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(2).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(0).child(2).child(1).setForeground(1, brush2)
        # other -> W2 -> Roof Fan and runner
        self.tw1.topLevelItem(0).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
        self.tw1.topLevelItem(0).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(3).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(0).child(3).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(3).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(0).child(3).child(1).setForeground(1, brush2)

        # other -> W2 -> 其它
        self.tw1.topLevelItem(0).child(4).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(0).child(4).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

        # other -> W3
        self.tw1.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).setForeground(1, brush3)
        # other -> W3 -> EAF 集塵
        self.tw1.topLevelItem(1).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        # other -> W3 -> 轉爐除塵
        self.tw1.topLevelItem(1).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(1).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(1).child(1).child(1).setForeground(1, brush2)
        self.tw1.topLevelItem(1).child(1).child(2).setForeground(1, brush2)
        self.tw1.topLevelItem(1).child(1).child(3).setForeground(1, brush2)
        self.tw1.topLevelItem(1).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(1).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        # other -> W4
        self.tw1.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(2).setForeground(1, brush3)
        # other -> W4 -> 型鋼,廠區
        self.tw1.topLevelItem(2).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(2).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(2).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(2).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

        # other -> W5
        self.tw1.topLevelItem(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).setForeground(1, brush3)
        # other -> W5 -> o2 #1
        self.tw1.topLevelItem(3).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(0).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(0).child(1).setForeground(1, brush2)

        # other -> W5 -> o2 #2
        self.tw1.topLevelItem(3).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(1).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(1).child(1).setForeground(1, brush2)
        # other -> W5 -> o2 #3
        self.tw1.topLevelItem(3).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(2).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(2).child(1).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(2).child(2).setForeground(1, brush2)
        # other -> W5 -> 空壓機群
        self.tw1.topLevelItem(3).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(4).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(5).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(6).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(7).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(4).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(5).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(6).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(7).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(4).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(5).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(6).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(7).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(3).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(1).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(2).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(3).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(4).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(5).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(6).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(3).child(7).setForeground(1, brush2)
        # other -> W5 -> IDF
        self.tw1.topLevelItem(3).child(4).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(4).child(0).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(4).child(1).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(4).child(2).setForeground(1, brush2)
        self.tw1.topLevelItem(3).child(4).child(3).setForeground(1, brush2)
        # other -> W5 -> 廠區用電
        self.tw1.topLevelItem(3).child(5).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(3).child(5).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        # other
        self.tw1.topLevelItem(4).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw1.topLevelItem(4).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(4).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw1.topLevelItem(4).setForeground(1, brush3)

        # 常調度負載
        self.tw2.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(4).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(5).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw2.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(4).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(5).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(4).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw2.topLevelItem(5).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

        # 發電 #1
        self.tw3.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw3.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

        # TGs,tg1~4 的第3~5 column
        self.tw3.topLevelItem(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

        # TRTs、CDQs
        self.tw3.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw3.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(1).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.tw3.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(2).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

    def initialize_tableWidget_3_colors(self):
        """ 設定 tableWidget_3 內特定行與欄位的背景顏色、文字顏色與對齊方式 """

        # 定義要修改的行與顏色
        color_mappings = {
            0: QtGui.QColor(80, 191, 200),  # 全廠用電量（第 0 列）
            1: QtGui.QColor(100, 170, 90),  # 中龍發電量（第 1 列）
            2: QtGui.QColor(170, 170, 0),  # 太陽能(直供)（第 2 列）
            3: QtGui.QColor(190, 90, 90),  # 台電供電量（第 3 列）
        }

        header_bg_color = QtGui.QColor(50, 50, 50)  # 深灰色背景
        header_text_color = QtGui.QColor(255, 255, 255)  # 白色文字
        text_color = QtGui.QColor(255, 255, 255)  # 白色文字
        font = QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold)

        # 美化表頭
        header = self.tableWidget_3.horizontalHeader()
        header.setStyleSheet(
            "QHeaderView::section {"
            "background-color: rgb(50, 50, 50);"  # 深灰色背景
            "color: white;"  # 文字顏色
            "font-size: 14px;"  # 字體大小
            "font-weight: bold;"  # 加粗
            "text-align: center;"  # 文字置中
            "border: 1px solid rgb(80, 80, 80);"  # 邊框顏色
            "}"
        )
        header.setFixedHeight(30)  # 設定表頭高度

        # 遍歷需要變色的行
        for row, bg_color in color_mappings.items():
            for col in [0, 1]:  # 只修改第 0 欄（名稱）和第 1 欄（即時量）
                item = self.tableWidget_3.item(row, col)
                if item is None:
                    item = QtWidgets.QTableWidgetItem()
                    self.tableWidget_3.setItem(row, col, item)

                # 設定顏色
                item.setBackground(QtGui.QBrush(bg_color))
                item.setForeground(QtGui.QBrush(text_color))
                item.setFont(font)

                # 設定對齊方式
                if col == 0:
                    item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)  # 名稱 → 置中
                else:
                    item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)  # 即時量 → 靠右

        self.tableWidget_3.viewport().update()  # 強制 UI 重新繪製

    def check_box2_event(self):
        #-----------調出當天的各週期平均-----------
        st = pd.Timestamp.today().date()
        et = st + pd.offsets.Day(1)
        self.dateEdit_3.setDate(QtCore.QDate(st.year, st.month, st.day))
        tw3_base_width = (self.tw3.columnWidth(0) + self.tw3.columnWidth(1) +20)
        base_width = self.tableWidget_3.columnWidth(0) + self.tableWidget_3.columnWidth(1)

        if self.checkBox_2.isChecked():     # 顯示歷史平均值
            self.history_demand_of_groups(st=st, et=et)
            #------function visible_____
            self.dateEdit_3.setVisible(True)
            self.horizontalScrollBar.setVisible(True)
            self.label_16.setVisible(True)
            self.label_17.setVisible(True)
            self.label_19.setVisible(True)
            self.label_21.setVisible(True)
            # ----------------------顯示平均值欄位，並增加 tree widget 總寬度 ----------------
            self.tw1.setColumnHidden(2, False)  # 隱藏模式必須先解除，columnWidth() 才能讀取到值
            self.tw2.setColumnHidden(2, False)
            self.tw3.setColumnHidden(2, False)
            tw1_width = self.tw1.columnWidth(0) + self.tw1.columnWidth(1) + self.tw1.columnWidth(2) + 20
            tw2_width = self.tw2.columnWidth(0) + self.tw2.columnWidth(1) + self.tw2.columnWidth(2)
            tw3_width = tw3_base_width + self.tw3.columnWidth(2)
            # ----------------------顯示平均值欄位，並增加 tablewidget3 總寬度 ----------------
            self.tableWidget_3.setColumnHidden(2, False)
            new_width = base_width + self.tableWidget_3.columnWidth(2)
        else:
            # ------function visible_____
            self.dateEdit_3.setVisible(False)
            self.horizontalScrollBar.setVisible(False)
            self.label_16.setVisible(False)
            self.label_17.setVisible(False)
            self.label_19.setVisible(False)
            self.label_21.setVisible(False)
            # ----------------------平均值欄位隱藏，並增加 tree widget 總寬度 ----------------
            tw1_width = self.tw1.columnWidth(0) + self.tw1.columnWidth(1) + 20
            tw2_width = self.tw2.columnWidth(0) + self.tw2.columnWidth(1)
            tw3_width = tw3_base_width
            self.tw1.setColumnHidden(2, True)
            self.tw2.setColumnHidden(2, True)
            self.tw3.setColumnHidden(2, True)
            # ----------------------顯示平均值欄位，並減少 tablewidget3 總寬度 ----------------
            self.tableWidget_3.setColumnHidden(2, True)
            new_width = base_width
        self.tw1.setFixedWidth(tw1_width)
        self.tw2.setFixedWidth(tw2_width)
        self.tw3.setFixedWidth(tw3_width)
        self.tableWidget_3.setFixedWidth(new_width)

    def check_box_event(self):
        """
        切換負載的顯示方式
        :return:
        """
        if self.checkBox.isChecked():
            self.tw1.topLevelItem(0).child(0).child(0).setText(0, '2H180')
            self.tw1.topLevelItem(0).child(0).child(1).setText(0, '2H280')
            self.tw1.topLevelItem(0).child(0).child(2).setText(0, '1H350')
            self.tw1.topLevelItem(0).child(1).setText(0, '4KA19')
            self.tw1.topLevelItem(0).child(2).child(0).setText(0, '4KB19')
            self.tw1.topLevelItem(0).child(2).child(1).setText(0, '4KB29')
            self.tw1.topLevelItem(0).child(3).child(0).setText(0, '2KA41')
            self.tw1.topLevelItem(0).child(3).child(1).setText(0, '2KB41')
            self.tw1.topLevelItem(1).child(0).setText(0, 'AJ320')
            self.tw1.topLevelItem(1).child(1).child(0).setText(0, '5KA18')
            self.tw1.topLevelItem(1).child(1).child(1).setText(0, '5KA28')
            self.tw1.topLevelItem(1).child(1).child(2).setText(0, '5KB18')
            self.tw1.topLevelItem(1).child(1).child(3).setText(0, '5KB28')
            self.tw1.topLevelItem(3).child(0).child(0).setText(0, '3KA14')
            self.tw1.topLevelItem(3).child(0).child(1).setText(0, '3KA15')
            self.tw1.topLevelItem(3).child(1).child(0).setText(0, '3KA24')
            self.tw1.topLevelItem(3).child(1).child(1).setText(0, '3KA25')
            self.tw1.topLevelItem(3).child(2).child(0).setText(0, '3KB12')
            self.tw1.topLevelItem(3).child(2).child(1).setText(0, '3KB22')
            self.tw1.topLevelItem(3).child(2).child(2).setText(0, '3KB28')
            self.tw1.topLevelItem(3).child(3).child(0).setText(0, '3KA16')
            self.tw1.topLevelItem(3).child(3).child(1).setText(0, '3KA26')
            self.tw1.topLevelItem(3).child(3).child(2).setText(0, '3KA17')
            self.tw1.topLevelItem(3).child(3).child(3).setText(0, '3KA27')
            self.tw1.topLevelItem(3).child(3).child(4).setText(0, '3KB16')
            self.tw1.topLevelItem(3).child(3).child(5).setText(0, '3KB26')
            self.tw1.topLevelItem(3).child(3).child(6).setText(0, '3KB17')
            self.tw1.topLevelItem(3).child(3).child(7).setText(0, '3KB27')
            self.tw1.topLevelItem(3).child(4).child(0).setText(0, '2KA19')
            self.tw1.topLevelItem(3).child(4).child(1).setText(0, '2KA29')
            self.tw1.topLevelItem(3).child(4).child(2).setText(0, '2KB19')
            self.tw1.topLevelItem(3).child(4).child(3).setText(0, '2KB29')
            self.tw2.topLevelItem(1).setText(0,'AH120')
            self.tw2.topLevelItem(2).setText(0,'AH190')
            self.tw2.topLevelItem(3).setText(0,'AH130')
            self.tw2.topLevelItem(4).setText(0,'1H360')
            self.tw2.topLevelItem(5).setText(0,'1H450')
            self.tw3.topLevelItem(0).child(0).setText(0, '2H120 & 2H220')
            self.tw3.topLevelItem(0).child(1).setText(0, '5H120 & 5H220')
            self.tw3.topLevelItem(0).child(2).setText(0, '1H120 & 1H220')
            self.tw3.topLevelItem(0).child(3).setText(0, '1H320 & 1H420')
            self.tw3.topLevelItem(1).child(0).setText(0, '4KA18')
            self.tw3.topLevelItem(1).child(1).setText(0, '5KB19')
            self.tw3.topLevelItem(2).child(0).setText(0, '4H120')
            self.tw3.topLevelItem(2).child(1).setText(0, '4H220')
        else:
            self.tw1.topLevelItem(0).child(0).child(0).setText(0, '#1 鼓風機')
            self.tw1.topLevelItem(0).child(0).child(1).setText(0, '#2 鼓風機')
            self.tw1.topLevelItem(0).child(0).child(2).setText(0, '#3 鼓風機')
            self.tw1.topLevelItem(0).child(1).setText(0, '#1 燒結風車')
            self.tw1.topLevelItem(0).child(2).child(0).setText(0, '#2-1')
            self.tw1.topLevelItem(0).child(2).child(1).setText(0, '#2-2')
            self.tw1.topLevelItem(0).child(3).child(0).setText(0, '#1')
            self.tw1.topLevelItem(0).child(3).child(1).setText(0, '#2')
            self.tw1.topLevelItem(1).child(0).setText(0, 'EAF 集塵')
            self.tw1.topLevelItem(1).child(1).child(0).setText(0, '#1')
            self.tw1.topLevelItem(1).child(1).child(1).setText(0, '#2')
            self.tw1.topLevelItem(1).child(1).child(2).setText(0, '#3')
            self.tw1.topLevelItem(1).child(1).child(3).setText(0, '#4')
            self.tw1.topLevelItem(3).child(0).child(0).setText(0, '1-1')
            self.tw1.topLevelItem(3).child(0).child(1).setText(0, '1-2')
            self.tw1.topLevelItem(3).child(1).child(0).setText(0, '2-1')
            self.tw1.topLevelItem(3).child(1).child(1).setText(0, '2-2')
            self.tw1.topLevelItem(3).child(2).child(0).setText(0, '3-1')
            self.tw1.topLevelItem(3).child(2).child(1).setText(0, '3-2')
            self.tw1.topLevelItem(3).child(2).child(2).setText(0, '3-3')
            self.tw1.topLevelItem(3).child(3).child(0).setText(0, '#1')
            self.tw1.topLevelItem(3).child(3).child(1).setText(0, '#2')
            self.tw1.topLevelItem(3).child(3).child(2).setText(0, '#3')
            self.tw1.topLevelItem(3).child(3).child(3).setText(0, '#4')
            self.tw1.topLevelItem(3).child(3).child(4).setText(0, '#5')
            self.tw1.topLevelItem(3).child(3).child(5).setText(0, '#6')
            self.tw1.topLevelItem(3).child(3).child(6).setText(0, '#7')
            self.tw1.topLevelItem(3).child(3).child(7).setText(0, '#8')
            self.tw1.topLevelItem(3).child(4).child(0).setText(0, 'IDF1 & BFP1,2')
            self.tw1.topLevelItem(3).child(4).child(1).setText(0, 'IDF2 & BFP3,4')
            self.tw1.topLevelItem(3).child(4).child(2).setText(0, 'IDF3 & BFP5,6')
            self.tw1.topLevelItem(3).child(4).child(3).setText(0, 'IDF4 & BFP7,8')
            self.tw2.topLevelItem(1).setText(0,'電爐')
            self.tw2.topLevelItem(2).setText(0,'#1 精煉爐')
            self.tw2.topLevelItem(3).setText(0,'#2 精煉爐')
            self.tw2.topLevelItem(4).setText(0,'#1 轉爐精煉爐')
            self.tw2.topLevelItem(5).setText(0,'#2 轉爐精煉爐')
            self.tw3.topLevelItem(0).child(0).setText(0, 'TG1')
            self.tw3.topLevelItem(0).child(1).setText(0, 'TG2')
            self.tw3.topLevelItem(0).child(2).setText(0, 'TG3')
            self.tw3.topLevelItem(0).child(3).setText(0, 'TG4')
            self.tw3.topLevelItem(1).child(0).setText(0, 'TRT#1')
            self.tw3.topLevelItem(1).child(1).setText(0, 'TRT#2')
            self.tw3.topLevelItem(2).child(0).setText(0, 'CDQ#1')
            self.tw3.topLevelItem(2).child(1).setText(0, 'CDQ#2')

    def dashboard_value(self):
        """
        1. 從 parameter.xlse 讀取出tag name 相關對照表, 轉換為list 指定給的 name_list這個變數
        2. tag_name 存成list當作search 的條件，找出符合條件的PIpoint 物件。(結果會存成list)
        3. 把 list 中的所有PIpoint 物件，取出其name、current_value 屬性，轉存在 DataFrame中。
        4. 透過 pd.merge() 的方法，做關聯式合併
        5. 從 buffer 這個dataframe 取出 value 這一列，而index 則採用name 這一列。
        6. 轉換 value 的資料型態 object->float，若遇文字型態，則用Nan 取代。
        7. 利用 group by 的功能，依Group1(單位)、Group2(負載類型)進行分組，將分組結果套入sum()的方法
        8. 使用slice (切片器) 來指定 MultiIndex 的範圍，指定各一級單位B類型(廠區用電)的計算結果，
           指定到wx 這個Series,並重新設定index
        9. 將wx 內容新增到c_values 之後。
        10. 獲取排程資料，並顯示在 tableWidget_4。
        11. current 排程顯示在第 1 列 (`start ~ end` 和 製程狀態)。
        12. future 排程顯示在後續列 (`start ~ end` 和 還剩幾分鐘開始)。
        13. 若 current 為空，則 future 從第 1 列開始顯示。
        :return:
        """
        name_list = self.tag_list['tag_name'].values.tolist()   # 1
        current = Pi.PIServer().search(name_list)    # 2
        buffer = pd.DataFrame([_.name, _.current_value] for _ in current)   # 3
        buffer.columns=['tag_name','value']
        buffer = pd.merge(self.tag_list, buffer, on='tag_name')      # 4
        buffer.loc[:,'value'] = pd.to_numeric(buffer.loc[:,'value'], errors='coerce') # 6

        c_values = buffer.loc[:,'value']
        c_values.index = buffer.loc[:,'name']     # 5

        wx_grouped = buffer.groupby(['Group1','Group2'])['value'].sum()     # 7
        wx = wx_grouped.loc[(slice('W2','WA')),'B']      # 8
        wx.index = wx.index.get_level_values(0)
        c_values = pd.concat([c_values, wx],axis=0)  # 9
        self.tws_update(c_values)

        """
        1. 獲取排程資料，並顯示在 tableWidget_4。
        2. current 排程顯示在第 1 列 (`start ~ end` 和 製程狀態)。
        3. future 排程顯示在後續列 (`start ~ end` 和 還剩幾分鐘開始)。
        4. 若 current 為空，則 future 從第 1 列開始顯示。
        5. **使用索引方式讀取 `entry`，確保相容性。**
        """
        # 取得排程資料
        past, current, future = scrapy_schedule()

        # 清空 tableWidget_4（保留格式）
        self.tableWidget_4.clearContents()

        # 設定行數
        total_rows = max(len(current), len(future))
        self.tableWidget_4.setRowCount(total_rows if total_rows > 0 else 1)

        # 設定標題列 (Header)
        self.tableWidget_4.setHorizontalHeaderLabels(["EAF 排程時間", "狀態"])
        self.tableWidget_4.setColumnWidth(0, 180)  # 調整欄寬
        self.tableWidget_4.setColumnWidth(1, 120)

        # 確保每行行高為35px
        for row in range(self.tableWidget_4.rowCount()):
            self.tableWidget_4.setRowHeight(row, 35)

        # 目前時間
        now = pd.Timestamp.now()

        row_index = 0  # 開始填入資料的列索引

        # 1️⃣ **顯示 current 排程**
        if current:
            for entry in current:
                start_time = entry[0].strftime("%H:%M:%S")  # 開始時間
                end_time = entry[1].strftime("%H:%M:%S")  # 結束時間
                process_status = entry[2]  # 爐別（A爐 / B爐）

                # 設定背景色 (淡黃色，標記 current)
                bg_color = QBrush(QColor(255, 245, 204))

                # 插入資料
                self.tableWidget_4.setItem(row_index, 0, QTableWidgetItem(f"{start_time} ~ {end_time}"))
                self.tableWidget_4.setItem(row_index, 1, QTableWidgetItem(process_status))

                # 設定格式（背景色 & 置中對齊）
                for col in range(2):
                    item = self.tableWidget_4.item(row_index, col)
                    item.setTextAlignment(4)  # Qt.AlignCenter
                    item.setBackground(bg_color)

                row_index += 1  # 更新 row 索引

        # 2️⃣ **顯示 future 排程**
        for entry in future:
            start_time = entry[0].strftime("%H:%M:%S")  # 開始時間
            end_time = entry[1].strftime("%H:%M:%S")  # 結束時間
            minutes_until_start = int((entry[0] - now).total_seconds() / 60)  # 計算距離開始時間（分鐘）

            self.tableWidget_4.setItem(row_index, 0, QTableWidgetItem(f"{start_time} ~ {end_time}"))
            self.tableWidget_4.setItem(row_index, 1, QTableWidgetItem(f"尚有 {minutes_until_start} 分鐘"))

            # 設定格式（置中對齊）
            for col in range(2):
                item = self.tableWidget_4.item(row_index, col)
                item.setTextAlignment(4)  # Qt.AlignCenter

            row_index += 1  # 更新 row 索引

        # 3️⃣ **若沒有排程，顯示 "目前無排程"**
        if row_index == 0:
            self.tableWidget_4.setItem(0, 0, QTableWidgetItem("目前無排程"))

        # 4️⃣ **美化表格：固定行高、禁止編輯、啟用選取**
        self.tableWidget_4.setEditTriggers(self.tableWidget_4.EditTrigger.NoEditTriggers)  # 禁止編輯
        self.tableWidget_4.setSelectionBehavior(self.tableWidget_4.SelectionBehavior.SelectRows)  # 選取整行
        self.tableWidget_4.verticalHeader().setDefaultSectionSize(30)  # 設定行高

    # @timeit
    def history_demand_of_groups(self, st, et):
        """
            查詢特定週期，各設備群組(分類)的平均值
        :return:
        """
        mask = ~pd.isnull(self.tag_list.loc[:,'tag_name2'])     # 作為用來篩選出tag中含有有kwh11 的布林索引器
        groups_demand = self.tag_list.loc[mask, 'tag_name2':'Group2']
        groups_demand.index = self.tag_list.loc[mask,'name']
        name_list = groups_demand.loc[:,'tag_name2'].values.tolist() # 把DataFrame 中標籤名為tag_name2 的值，轉成list輸出
        query_result = query_pi(st=st, et=et, tags=name_list ,extract_type = 16)

        query_result.columns = groups_demand.index
        query_result = query_result.T       # 將query_result 轉置 shape:(96,178) -> (178,96)
        query_result.reset_index(inplace=True, drop=True)  # 重置及捨棄原本的 index
        query_result.index = groups_demand.index    # 將index 更新為各迴路或gas 的名稱 (套用groups_demands.index 即可)
        time_list = [t.strftime('%H:%M') for t in  pd.date_range('00:00', '23:45', freq='15min')]
        query_result.columns = time_list        # 用週期的起始時間，作為各column 的名稱
        query_result.loc[:,'00:00':'23:45'] = query_result.loc[:,'00:00':'23:45'] * 4 # kwh -> MW/15 min
        groups_demand = pd.concat([groups_demand, query_result], axis=1, copy=False)
        wx_list = list()    # 暫存各wx的計算結果用
        for _ in time_list:
            # 利用 group by 的功能，依Group1(單位)、Group2(負載類型)進行分組，將分組結果套入sum()的方法
            wx_grouped = groups_demand.groupby(['Group1','Group2'])[_].sum()
            c = wx_grouped.loc['W2':'WA', 'B']
            c.name = _
            c.index = c.index.get_level_values(0)   # 重新將index 設置為原multiIndex 的第一層index 內容
            wx_list.append(c)
        wx = pd.DataFrame([wx_list[_] for _ in range(96)])
        # 將wx 計算結果轉置，並along index 合併於groups_demand 下方, 並將結果存在class 變數中
        self.history_datas_of_groups = pd.concat([groups_demand, wx.T], axis=0)

    def date_edit3_user_change(self):
        if self.dateEdit_3.date() > pd.Timestamp.today().date():
            # ----選定到未來日期時，查詢當天的各週期資料，並顯示最後一個結束週期的資料----
            sd = pd.Timestamp(pd.Timestamp.now().date())
            self.dateEdit_3.blockSignals(True)  # 屏蔽dateEdit 的signal, 避免無限執行
            self.dateEdit_3.setDate(QtCore.QDate(sd.year, sd.month, sd.day))
            self.dateEdit_3.blockSignals(False) # 設定完dateEdit 後重新開啟DateEdit 的signal
            ed = sd + pd.offsets.Day(1)
            self.history_demand_of_groups(st=sd, et=ed)

            # 將et 設定在最接近目前時間點之前的最後15分鐘結束點, 並將 scrollerBar 調整至相對應的值
            # 並觸發scrollerBar 的value changed 事件，執行後續動作。
            sp = pd.Timestamp.now().floor('15T')
            self.horizontalScrollBar.setValue((sp - pd.Timestamp.now().normalize()) // pd.Timedelta('15T')-1)

        else:
            # ------ 初始帶入或選擇非未來日期時，查詢完資料後，顯示第一個週期的資料
            sd = pd.Timestamp(self.dateEdit_3.date().toString())
            ed = sd + pd.offsets.Day(1)
            self.history_demand_of_groups(st=sd, et=ed)
            self.label_16.setText('00:00')
            self.label_17.setText('00:15')
            self.update_history_to_tws(self.history_datas_of_groups.loc[:, '00:00'])
            self.horizontalScrollBar.setValue(0)

    def confirm_value(self):
        """scrollbar 數值變更後，判斷是否屬於未來時間，並依不同狀況執行相對應的區間、紀錄顯示"""
        st = pd.Timestamp(self.dateEdit_3.date().toString()) + pd.offsets.Minute(15) * self.horizontalScrollBar.value()
        et = st + pd.offsets.Minute(15)

        if et > pd.Timestamp.now():     # 欲查詢的時間段，屬於未來時間時
            # 將et 設定在最接近目前時間點之前的最後15分鐘結束點, 並將 scrollerBar 調整至相對應的值,
            et = pd.Timestamp.now().floor('15T')
            self.horizontalScrollBar.setValue((et - pd.Timestamp.now().normalize()) // pd.Timedelta('15T')-1)
            st = et - pd.offsets.Minute(15)

        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))
        self.update_history_to_tws(self.history_datas_of_groups.loc[:,st.strftime('%H:%M')])

    def update_history_to_tws(self, current_p):
        """
        暫時用來將各群組的歷史平均量顯顯示在 各tree widget 的3rd column
        :param current_p:
        :return:
        """
        w2_total = current_p['2H180':'2KB41'].sum() + current_p['W2']
        self.tw1.topLevelItem(0).setText(2, pre_check2(w2_total))
        self.tw1.topLevelItem(0).child(0).setText(2, pre_check2(current_p['2H180':'1H350'].sum()))
        self.tw1.topLevelItem(0).child(0).child(0).setText(2, pre_check2(current_p['2H180']))
        self.tw1.topLevelItem(0).child(0).child(1).setText(2, pre_check2(current_p['2H280']))
        self.tw1.topLevelItem(0).child(0).child(2).setText(2, pre_check2(current_p['1H350']))
        self.tw1.topLevelItem(0).child(1).setText(2, pre_check2(current_p['4KA19']))
        self.tw1.topLevelItem(0).child(2).setText(2, pre_check2(current_p['4KB19':'4KB29'].sum()))
        self.tw1.topLevelItem(0).child(2).child(0).setText(2, pre_check2(current_p['4KB19']))
        self.tw1.topLevelItem(0).child(2).child(1).setText(2, pre_check2(current_p['4KB29']))
        self.tw1.topLevelItem(0).child(3).setText(1, pre_check2(current_p['2KA41':'2KB41'].sum()))
        self.tw1.topLevelItem(0).child(3).child(0).setText(2, pre_check2(current_p['2KA41']))
        self.tw1.topLevelItem(0).child(3).child(1).setText(2, pre_check2(current_p['2KB41']))
        self.tw1.topLevelItem(0).child(4).setText(2, pre_check2(current_p['W2']))

        w3_total = current_p['AJ320':'5KB28'].sum() + current_p['W3']
        self.tw1.topLevelItem(1).setText(2, pre_check2(w3_total))
        self.tw1.topLevelItem(1).child(0).setText(2, pre_check2(current_p['AJ320']))
        self.tw1.topLevelItem(1).child(1).setText(2, pre_check2(current_p['5KA18':'5KB28'].sum()))
        self.tw1.topLevelItem(1).child(1).child(0).setText(2, pre_check2(current_p['5KA18']))
        self.tw1.topLevelItem(1).child(1).child(1).setText(2, pre_check2(current_p['5KA28']))
        self.tw1.topLevelItem(1).child(1).child(2).setText(2, pre_check2(current_p['5KB18']))
        self.tw1.topLevelItem(1).child(1).child(3).setText(2, pre_check2(current_p['5KB28']))
        self.tw1.topLevelItem(1).child(2).setText(2, pre_check2(current_p['W3']))

        w42 = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_total = current_p['AJ130':'AJ320'].sum() + w42

        self.tw1.topLevelItem(2).setText(2, pre_check2(w4_total))
        self.tw1.topLevelItem(2).child(0).setText(2, pre_check2(current_p['AJ130':'AJ320'].sum()))
        self.tw1.topLevelItem(2).child(1).setText(2, pre_check2(w42))

        w5_total = current_p['3KA14':'2KB29'].sum() + current_p['W5']
        self.tw1.topLevelItem(3).setText(2,pre_check2(w5_total))
        self.tw1.topLevelItem(3).child(0).setText(2, pre_check2(current_p['3KA14':'3KA15'].sum()))
        self.tw1.topLevelItem(3).child(0).child(0).setText(2, pre_check2(current_p['3KA14']))
        self.tw1.topLevelItem(3).child(0).child(1).setText(2, pre_check2(current_p['3KA15']))
        self.tw1.topLevelItem(3).child(1).setText(2, pre_check2(current_p['3KA24':'3KA25'].sum()))
        self.tw1.topLevelItem(3).child(1).child(0).setText(2, pre_check2(current_p['3KA24']))
        self.tw1.topLevelItem(3).child(1).child(1).setText(2, pre_check2(current_p['3KA25']))
        self.tw1.topLevelItem(3).child(2).setText(2, pre_check2(current_p['3KB12':'3KB28'].sum()))
        self.tw1.topLevelItem(3).child(2).child(0).setText(2, pre_check2(current_p['3KB12']))
        self.tw1.topLevelItem(3).child(2).child(1).setText(2, pre_check2(current_p['3KB22']))
        self.tw1.topLevelItem(3).child(2).child(2).setText(2, pre_check2(current_p['3KB28']))
        self.tw1.topLevelItem(3).child(3).setText(2, pre_check2(current_p['3KA16':'3KB27'].sum()))
        self.tw1.topLevelItem(3).child(3).child(0).setText(2, pre_check2(current_p['3KA16']))
        self.tw1.topLevelItem(3).child(3).child(1).setText(2, pre_check2(current_p['3KA26']))
        self.tw1.topLevelItem(3).child(3).child(2).setText(2, pre_check2(current_p['3KA17']))
        self.tw1.topLevelItem(3).child(3).child(3).setText(2, pre_check2(current_p['3KA27']))
        self.tw1.topLevelItem(3).child(3).child(4).setText(2, pre_check2(current_p['3KB16']))
        self.tw1.topLevelItem(3).child(3).child(5).setText(2, pre_check2(current_p['3KB26']))
        self.tw1.topLevelItem(3).child(3).child(6).setText(2, pre_check2(current_p['3KB17']))
        self.tw1.topLevelItem(3).child(3).child(7).setText(2, pre_check2(current_p['3KB27']))
        self.tw1.topLevelItem(3).child(4).setText(2, pre_check2(current_p['2KA19':'2KB29'].sum()))
        self.tw1.topLevelItem(3).child(4).child(0).setText(2, pre_check2(current_p['2KA19']))
        self.tw1.topLevelItem(3).child(4).child(1).setText(2, pre_check2(current_p['2KA29']))
        self.tw1.topLevelItem(3).child(4).child(2).setText(2, pre_check2(current_p['2KB19']))
        self.tw1.topLevelItem(3).child(4).child(3).setText(2, pre_check2(current_p['2KB29']))
        self.tw1.topLevelItem(3).child(5).setText(2, pre_check2(current_p['W5']))
        self.tw1.topLevelItem(4).setText(2, pre_check2(current_p['WA']))
        #other=w2_total+w3_total+w4_total+w5_total+current_p['WA']
        #self.label_17.setText(str(other))

        self.tw2.topLevelItem(0).setText(2, pre_check2(current_p['9H140':'9KB33'].sum(),b=0))
        self.tw2.topLevelItem(1).setText(2, pre_check2(current_p['AH120'],b=0))
        self.tw2.topLevelItem(2).setText(2, pre_check2(current_p['AH190'],b=0))
        self.tw2.topLevelItem(3).setText(2, pre_check2(current_p['AH130'],b=0))
        self.tw2.topLevelItem(4).setText(2, pre_check2(current_p['1H360'],b=0))
        self.tw2.topLevelItem(5).setText(2, pre_check2(current_p['1H450'],b=0))

        self.tw3.topLevelItem(0).setText(2, pre_check2(current_p['2H120':'1H420'].sum()))
        self.tw3.topLevelItem(0).child(0).setText(2, pre_check2(current_p['2H120':'2H220'].sum()))
        self.tw3.topLevelItem(0).child(1).setText(2, pre_check2(current_p['5H120':'5H220'].sum()))
        self.tw3.topLevelItem(0).child(2).setText(2, pre_check2(current_p['1H120':'1H220'].sum()))
        self.tw3.topLevelItem(0).child(3).setText(2, pre_check2(current_p['1H320':'1H420'].sum()))

        self.tw3.topLevelItem(1).setText(2, pre_check2(current_p['4KA18':'5KB19'].sum()))
        self.tw3.topLevelItem(1).child(0).setText(2, pre_check2(current_p['4KA18']))
        self.tw3.topLevelItem(1).child(1).setText(2, pre_check2(current_p['5KB19']))
        self.tw3.topLevelItem(2).setText(2, pre_check2(current_p['4H120':'4H220'].sum()))
        self.tw3.topLevelItem(2).child(0).setText(2, pre_check2(current_p['4H120']))
        self.tw3.topLevelItem(2).child(1).setText(2, pre_check2(current_p['4H220']))

        sun_power = current_p['9KB25-4_2':'3KA12-1_2'].sum()
        tai_power = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() - sun_power

        # 方式 2：table widget 3 利用 self.update_and_style_table_item 函式，在更新內容後，重新套用樣式
        self.update_and_style_table_item(self.tableWidget_3, 0, 2, pre_check2(tai_power))
        self.update_and_style_table_item(self.tableWidget_3,1 ,2, pre_check2(current_p['2H120':'5KB19'].sum()))
        self.update_and_style_table_item(self.tableWidget_3, 2, 2, pre_check2(sun_power,b=5))
        self.update_and_style_table_item(self.tableWidget_3, 3, 2, pre_check2(current_p['feeder 1510':'feeder 1520'].sum(),b=4))

    def tws_update(self, current_p):
        """
        更新樹狀結構(tree widget)、表格結構(table widget) 裡的資料
        :param current_p: 即時用電量。pd.Series
        :return:
        """
        # brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)   # 設定顏色的分佈方式
        # self.treeWidget.headerItem().setForeground(0,brush) # 設置表頭項目的字體顏色
        # self.tw1.topLevelItem(0).setTextAlignment(0, 1 | 4)
        w2_total = current_p['2H180':'2KB41'].sum() + current_p['W2']
        self.tw1.topLevelItem(0).setText(1, pre_check(w2_total))
        self.tw1.topLevelItem(0).child(0).setText(1, pre_check(current_p['2H180':'1H350'].sum()))
        self.tw1.topLevelItem(0).child(0).child(0).setText(1, pre_check(current_p['2H180']))
        self.tw1.topLevelItem(0).child(0).child(1).setText(1, pre_check(current_p['2H280']))
        self.tw1.topLevelItem(0).child(0).child(2).setText(1, pre_check(current_p['1H350']))
        self.tw1.topLevelItem(0).child(1).setText(1, pre_check(current_p['4KA19']))
        self.tw1.topLevelItem(0).child(2).setText(1, pre_check(current_p['4KB19':'4KB29'].sum()))
        self.tw1.topLevelItem(0).child(2).child(0).setText(1, pre_check(current_p['4KB19']))
        self.tw1.topLevelItem(0).child(2).child(1).setText(1, pre_check(current_p['4KB29']))
        self.tw1.topLevelItem(0).child(3).setText(1, pre_check(current_p['2KA41':'2KB41'].sum()))
        self.tw1.topLevelItem(0).child(3).child(0).setText(1, pre_check(current_p['2KA41']))
        self.tw1.topLevelItem(0).child(3).child(1).setText(1, pre_check(current_p['2KB41']))
        self.tw1.topLevelItem(0).child(4).setText(1, pre_check(current_p['W2']))

        w3_total = current_p['AJ320':'5KB28'].sum() + current_p['W3']
        self.tw1.topLevelItem(1).setText(1, pre_check(w3_total))
        self.tw1.topLevelItem(1).child(0).setText(1, pre_check(current_p['AJ320']))
        self.tw1.topLevelItem(1).child(1).setText(1, pre_check(current_p['5KA18':'5KB28'].sum()))
        self.tw1.topLevelItem(1).child(1).child(0).setText(1, pre_check(current_p['5KA18']))
        self.tw1.topLevelItem(1).child(1).child(1).setText(1, pre_check(current_p['5KA28']))
        self.tw1.topLevelItem(1).child(1).child(2).setText(1, pre_check(current_p['5KB18']))
        self.tw1.topLevelItem(1).child(1).child(3).setText(1, pre_check(current_p['5KB28']))
        self.tw1.topLevelItem(1).child(2).setText(1, pre_check(current_p['W3']))

        w42 = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_total = current_p['AJ130':'AJ320'].sum() + w42

        self.tw1.topLevelItem(2).setText(1, pre_check(w4_total))
        self.tw1.topLevelItem(2).child(0).setText(1, pre_check(current_p['AJ130':'AJ320'].sum()))
        self.tw1.topLevelItem(2).child(1).setText(1, pre_check(w42))

        w5_total = current_p['3KA14':'2KB29'].sum() + current_p['W5']
        self.tw1.topLevelItem(3).setText(1,pre_check(w5_total))
        self.tw1.topLevelItem(3).child(0).setText(1, pre_check(current_p['3KA14':'3KA15'].sum()))
        self.tw1.topLevelItem(3).child(0).child(0).setText(1, pre_check(current_p['3KA14']))
        self.tw1.topLevelItem(3).child(0).child(1).setText(1, pre_check(current_p['3KA15']))
        self.tw1.topLevelItem(3).child(1).setText(1, pre_check(current_p['3KA24':'3KA25'].sum()))
        self.tw1.topLevelItem(3).child(1).child(0).setText(1, pre_check(current_p['3KA24']))
        self.tw1.topLevelItem(3).child(1).child(1).setText(1, pre_check(current_p['3KA25']))
        self.tw1.topLevelItem(3).child(2).setText(1, pre_check(current_p['3KB12':'3KB28'].sum()))
        self.tw1.topLevelItem(3).child(2).child(0).setText(1, pre_check(current_p['3KB12']))
        self.tw1.topLevelItem(3).child(2).child(1).setText(1, pre_check(current_p['3KB22']))
        self.tw1.topLevelItem(3).child(2).child(2).setText(1, pre_check(current_p['3KB28']))
        self.tw1.topLevelItem(3).child(3).setText(1, pre_check(current_p['3KA16':'3KB27'].sum()))
        self.tw1.topLevelItem(3).child(3).child(0).setText(1, pre_check(current_p['3KA16']))
        self.tw1.topLevelItem(3).child(3).child(1).setText(1, pre_check(current_p['3KA26']))
        self.tw1.topLevelItem(3).child(3).child(2).setText(1, pre_check(current_p['3KA17']))
        self.tw1.topLevelItem(3).child(3).child(3).setText(1, pre_check(current_p['3KA27']))
        self.tw1.topLevelItem(3).child(3).child(4).setText(1, pre_check(current_p['3KB16']))
        self.tw1.topLevelItem(3).child(3).child(5).setText(1, pre_check(current_p['3KB26']))
        self.tw1.topLevelItem(3).child(3).child(6).setText(1, pre_check(current_p['3KB17']))
        self.tw1.topLevelItem(3).child(3).child(7).setText(1, pre_check(current_p['3KB27']))
        self.tw1.topLevelItem(3).child(4).setText(1, pre_check(current_p['2KA19':'2KB29'].sum()))
        self.tw1.topLevelItem(3).child(4).child(0).setText(1, pre_check(current_p['2KA19']))
        self.tw1.topLevelItem(3).child(4).child(1).setText(1, pre_check(current_p['2KA29']))
        self.tw1.topLevelItem(3).child(4).child(2).setText(1, pre_check(current_p['2KB19']))
        self.tw1.topLevelItem(3).child(4).child(3).setText(1, pre_check(current_p['2KB29']))
        self.tw1.topLevelItem(3).child(5).setText(1, pre_check(current_p['W5']))
        self.tw1.topLevelItem(4).setText(1, pre_check(current_p['WA']))
        #other=w2_total+w3_total+w4_total+w5_total+current_p['WA']
        #self.label_17.setText(str(other))

        self.tw2.topLevelItem(0).setText(1, pre_check(current_p['9H140':'9KB33'].sum(), 0))
        self.tw2.topLevelItem(1).setText(1, pre_check(current_p['AH120'], 0))
        self.tw2.topLevelItem(2).setText(1, pre_check(current_p['AH190'], 0))
        self.tw2.topLevelItem(3).setText(1, pre_check(current_p['AH130'],0))
        self.tw2.topLevelItem(4).setText(1, pre_check(current_p['1H360'], 0))
        self.tw2.topLevelItem(5).setText(1, pre_check(current_p['1H450'], 0))

        ng_to_power = self.unit_prices.loc['可轉換電力', 'current']

        # tw 的總寬度 = columnWidth(0..1..2) lineWidth() frameWidth()
        self.tw3.topLevelItem(0).setText(1, pre_check(current_p['2H120':'1H420'].sum()))
        self.tw3.topLevelItem(0).child(0).setText(1, pre_check(current_p['2H120':'2H220'].sum()))
        self.tw3.topLevelItem(0).child(1).setText(1, pre_check(current_p['5H120':'5H220'].sum()))
        self.tw3.topLevelItem(0).child(2).setText(1, pre_check(current_p['1H120':'1H220'].sum()))
        self.tw3.topLevelItem(0).child(3).setText(1, pre_check(current_p['1H320':'1H420'].sum()))
        # tw3 的TGs 及其子節點 TG1~TG4 的 NG貢獻電量、使用量，從原本顯示在最後兩個column，改為顯示在3rd 的tip
        ng = pd.Series([current_p['TG1 NG':'TG4 NG'].sum(), current_p['TG1 NG'], current_p['TG2 NG'],
                        current_p['TG3 NG'], current_p['TG4 NG'], ng_to_power])
        self.update_tw3_tips_and_colors(ng)
        self.tw3.topLevelItem(1).setText(1, pre_check(current_p['4KA18':'5KB19'].sum()))
        self.tw3.topLevelItem(1).child(0).setText(1, pre_check(current_p['4KA18']))
        self.tw3.topLevelItem(1).child(1).setText(1, pre_check(current_p['5KB19']))
        self.tw3.topLevelItem(2).setText(1, pre_check(current_p['4H120':'4H220'].sum()))
        self.tw3.topLevelItem(2).child(0).setText(1, pre_check(current_p['4H120']))
        self.tw3.topLevelItem(2).child(1).setText(1, pre_check(current_p['4H220']))


        tai_power = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
                    - current_p['sp_real_time']
        # 方式 2：table widget 3 利用 self.update_table_item 函式，在更新內容後，保留原本樣式不變
        self.update_table_item(self.tableWidget_3, 0, 1, pre_check(tai_power))
        self.update_table_item(self.tableWidget_3, 1, 1, pre_check(current_p['2H120':'5KB19'].sum()))
        self.update_table_item(self.tableWidget_3, 2, 1, pre_check(current_p['sp_real_time'], b=5))
        self.update_table_item(self.tableWidget_3, 3, 1, pre_check2(current_p['feeder 1510':'feeder 1520'].sum(), b=4))

    def update_tw3_tips_and_colors(self, ng):
        """
        更新 tw3 (QTreeWidget) 中 TGs 及其子節點 TG1~TG4 的 2nd column (即時量)，
        設定美化的 Tooltip，並根據 NG 貢獻電量改變顏色。

        參數:
            ng (pd.Series): NG 數據, 來源外部
        """

        tg_item = self.tw3.topLevelItem(0)  # TGs 節點

        # 定義顏色
        default_color = QtGui.QColor(0, 0, 0)  # 黑色 (預設)
        highlight_color = QtGui.QColor(255, 0, 0)  # 紅色 (NG 貢獻電量 > 0)

        # 取得 Nm3/hr 轉 MW 的係數
        conversion_factor = ng[5]

        # 計算 TGs 的 NG 貢獻電量
        tgs_ng_contribution = (ng[0] * conversion_factor) / 1000

        # 設定 TGs 的美化 Tip 訊息
        tgs_tooltip = f"""
        <div style="background-color:#FFFFCC; padding:5px; border-radius:5px;">
            <b>NG 使用量:</b> <span style="color:#0000FF;">{ng[0]:.2f} Nm³/hr</span><br>
            <b>NG 貢獻電量:</b> <span style="color:#FF0000;">{tgs_ng_contribution:.2f} MW</span>
        </div>
        """
        tg_item.setToolTip(1, tgs_tooltip)  # TGs 的即時量 Tooltip

        # 變更 TGs 的字體顏色
        tg_item.setForeground(1, QtGui.QBrush(highlight_color if tgs_ng_contribution > 0 else default_color))

        # 遍歷 TG1 ~ TG4
        for i in range(tg_item.childCount()):
            tg_child = tg_item.child(i)

            # 取得 NG 使用量
            ng_usage = ng[i + 1]  # TG1~TG4 NG 使用量

            # 計算 NG 貢獻電量
            ng_contribution = (ng_usage * conversion_factor) / 1000

            # 設定美化的 Tip 訊息
            tooltip_text = f"""
            <div style="background-color:#F0F0F0; padding:5px; border-radius:5px;">
                <b>NG 使用量:</b> <span style="color:#0000FF;">{ng_usage:.2f} Nm³/hr</span><br>
                <b>NG 貢獻電量:</b> <span style="color:#FF0000;">{ng_contribution:.2f} MW</span>
            </div>
            """
            tg_child.setToolTip(1, tooltip_text)  # 針對 2nd column (即時量) 設定美化 Tooltip

            # 變更字體顏色
            tg_child.setForeground(1, QtGui.QBrush(highlight_color if ng_contribution > 0 else default_color))

    def update_current_value(self):
        """
        用來每隔11秒，自動更新current value
        :return:
        """
        while True:
            self.dashboard_value()
            time.sleep(11)

    @staticmethod
    def apply_style_to_item(item):
        """ 套用固定的樣式到 QTableWidgetItem """
        item.setFont(QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
        item.setBackground(QtGui.QBrush(QtGui.QColor("#FFFACD")))  # 淡黃色背景
        item.setForeground(QtGui.QBrush(QtGui.QColor("#000080")))  # 深藍色文字
        item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

    def update_table_item(self, table_widget, row, column, new_text):
        """ 更新 QTableWidget 的指定儲存格內容，同時保留原來的樣式 """
        item = table_widget.item(row, column)
        if item is None:  # 如果該儲存格沒有 item，則創建一個新的 item
            item = QtWidgets.QTableWidgetItem()
            table_widget.setItem(row, column, item)

        item.setText(new_text)  # 只更新內容，不改變格式

    def update_and_style_table_item(self, table_widget, row, column, new_text):
        """ 更新 QTableWidget 的儲存格內容，並確保樣式不會變 """
        item = table_widget.item(row, column)
        if item is None:  # 若 item 不存在，創建並套用樣式
            item = QtWidgets.QTableWidgetItem(new_text)
            self.apply_style_to_item(item)
            table_widget.setItem(row, column, item)
        else:
            item.setText(new_text)  # 只修改文字
            self.apply_style_to_item(item)  # 重新套用樣式

    def beautify_avg_column(self, widget, column_index):
        """
        用來美化treeWidget、tableWidget 的平块值欄位
        :param widget: 用來接收傳入的widget
        :param column_index: 要修改的 column欄位編號
        :return:
        """
        if isinstance(widget, QtWidgets.QTreeWidget):  # 若為 QTreeWidget
            for i in range(widget.topLevelItemCount()):
                item = widget.topLevelItem(i)
                if item:
                    item.setFont(column_index, QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
                    item.setBackground(column_index, QtGui.QBrush(QtGui.QColor("#FFFACD")))  # 淡黃色背景
                    item.setForeground(column_index, QtGui.QBrush(QtGui.QColor("#000080")))  # 深藍色文字
                    item.setTextAlignment(column_index, QtCore.Qt.AlignmentFlag.AlignRight)
                    # 遞迴處理子節點
                    self.beautify_avg_children(item, column_index)

        elif isinstance(widget, QtWidgets.QTableWidget):  # 若為 QTableWidget
            for row in range(widget.rowCount()):
                item = widget.item(row, column_index)
                if item:
                    item.setFont(QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
                    item.setBackground(QtGui.QBrush(QtGui.QColor("#FFFACD")))
                    item.setForeground(QtGui.QBrush(QtGui.QColor("#000080")))
                    item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

    def beautify_avg_children(self, parent_item, column_index):
        """遞迴處理 QTreeWidgetItem 子節點"""
        for i in range(parent_item.childCount()):
            child = parent_item.child(i)
            if child:
                child.setFont(column_index, QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
                child.setBackground(column_index, QtGui.QBrush(QtGui.QColor("#FFFACD")))
                child.setForeground(column_index, QtGui.QBrush(QtGui.QColor("#000080")))
                child.setTextAlignment(column_index, QtCore.Qt.AlignmentFlag.AlignRight)
                self.beautify_avg_children(child, column_index)  # 遞迴處理下一層

    def tw3_expanded_event(self):
        """
        1. 用來同步TGs 發電量、NG貢獻電量、NG使用量的項目展開、收縮
        2. 所有項目在expanded 或 collapsed 時，變更文字顯示的方式
        :return:
        """
        b_transparent = QtGui.QBrush(QtGui.QColor(0,0,0,0))
        b_solid  = QtGui.QBrush(QtGui.QColor(0,0,0, 255))
        # TGs
        if self.tw3.topLevelItem(0).isExpanded():
            self.tw3.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(0).setForeground(1, b_transparent)
        else:
            self.tw3.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(0).setForeground(1, b_solid)
        # TRTs
        if self.tw3.topLevelItem(1).isExpanded():
            self.tw3.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(1).setForeground(1, b_transparent)
        else:
            self.tw3.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(1).setForeground(1, b_solid)
        # CDQs
        if self.tw3.topLevelItem(2).isExpanded():
            self.tw3.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(2).setForeground(1, b_transparent)
        else:
            self.tw3.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(2).setForeground(1, b_solid)

    def tw1_expanded_event(self):
        """
        For the event of tw1 about being expanded or collapsed
        :return:
        """
        b_transparent = QtGui.QBrush(QtGui.QColor(0,0,0,0))
        b_solid  = QtGui.QBrush(QtGui.QColor(0,0,0, 255))

        # w2
        if self.tw1.topLevelItem(0).isExpanded():
            self.tw1.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw1.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw1.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw1.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        # w2 --> 鼓風機群
        if self.tw1.topLevelItem(0).child(0).isExpanded():
            self.tw1.topLevelItem(0).child(0).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(0).child(0).setForeground(1, b_solid)
        # w2 --> #2 燒結風車群
        if self.tw1.topLevelItem(0).child(2).isExpanded():
            self.tw1.topLevelItem(0).child(2).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(0).child(2).setForeground(1, b_solid)
        # w2 --> #2 屋頂風扇&runner 群
        if self.tw1.topLevelItem(0).child(3).isExpanded():
            self.tw1.topLevelItem(0).child(3).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(0).child(3).setForeground(1, b_solid)

        # w3
        if self.tw1.topLevelItem(1).isExpanded():
            self.tw1.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw1.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw1.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw1.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

        # w3 --> 轉爐除塵
        if self.tw1.topLevelItem(1).child(1).isExpanded():
            self.tw1.topLevelItem(1).child(1).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(1).child(1).setForeground(1, b_solid)
        # w4
        if self.tw1.topLevelItem(2).isExpanded():
            self.tw1.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw1.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw1.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw1.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        # w5
        if self.tw1.topLevelItem(3).isExpanded():
            self.tw1.topLevelItem(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw1.topLevelItem(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw1.topLevelItem(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw1.topLevelItem(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

        # w5 --> O2#1
        if self.tw1.topLevelItem(3).child(0).isExpanded():
            self.tw1.topLevelItem(3).child(0).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(3).child(0).setForeground(1, b_solid)
        # w5 --> O2#2
        if self.tw1.topLevelItem(3).child(1).isExpanded():
            self.tw1.topLevelItem(3).child(1).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(3).child(1).setForeground(1, b_solid)
        # w5 --> O2#3
        if self.tw1.topLevelItem(3).child(2).isExpanded():
            self.tw1.topLevelItem(3).child(2).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(3).child(2).setForeground(1, b_solid)
        # w5 --> 空壓機群
        if self.tw1.topLevelItem(3).child(3).isExpanded():
            self.tw1.topLevelItem(3).child(3).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(3).child(3).setForeground(1, b_solid)
        # w5 --> IDF 群
        if self.tw1.topLevelItem(3).child(4).isExpanded():
            self.tw1.topLevelItem(3).child(4).setForeground(1, b_transparent)
        else:
            self.tw1.topLevelItem(3).child(4).setForeground(1, b_solid)

    def handle_selection_changed(self):
        """
        1. 以list的方式返回被選擇的item
        2. 排除非需量或空白字的 cell
        :return:
        """
        a = self.tableWidget_2.selectedItems()  # 1
        sum_of_selection = list()
        for i in range(len(a)):     # 2
            if (a[i].column() % 2 != 0) & (a[i].text() != ''):
                sum_of_selection.append(a[i].text())
        b = pd.Series(sum_of_selection, dtype=float)
        self.label_6.setText(str(b.mean()))
        self.label_8.setText(str(len(b)))

    def query_demand(self):
        """
        此函式的功能為查詢指定日期的週期需量。
        1. j -> Column    i -> row
        2. item1 用來設定和起始時間有關的cell；item2 用來設定和需量有關cell。
        3. 設定item 內容的字體大小
        4. 將item 內容置中
        5. 判斷raw_data 中是否有nan 值，如果是，則將該item 內容設為空白字串
        6. 判斷該週期的結束時間，是否大於current time。  (True:字體紅色  False:字體藍色)
        7. 將表格的高度、寬度自動依內容調整
        :return:
        """
        tags=('W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11')
        st = pd.Timestamp(str(self.dateEdit.date().toPyDate()))
        et = st + pd.offsets.Day(1)
        raw_data = query_pi(st=st, et=et, tags=tags, extract_type=16)
        raw_data.insert(0, 'TPC', (raw_data.iloc[:, 0] + raw_data.iloc[:, 1]) * 4)
        for j in range(6):          # 1
            for i in range(16):
                item1 = QtWidgets.QTableWidgetItem(pd.Timestamp(raw_data.index[i + j * 16]).strftime('%H:%M'))  #2
                font = QtGui.QFont()
                font.setPointSize(10)
                item1.setFont(font)         # 3
                self.tableWidget_2.setItem(i, 0 + j * 2,item1)
                self.tableWidget_2.item(i, 0 + j * 2).setTextAlignment(4 | 4)       # 4

                if pd.isnull(raw_data.iloc[i + j * 16, 0]):             # 5
                    item2 = QtWidgets.QTableWidgetItem(str(''))
                else:
                    item2 = QtWidgets.QTableWidgetItem(str(round(raw_data.iloc[i + j * 16,0], 3)))
                if pd.Timestamp.now() < (raw_data.index[i + j * 16].tz_localize(None) + pd.offsets.Minute(15)):
                    brush = QtGui.QBrush(QtGui.QColor(255, 0, 0))       # 6
                else:
                    brush = QtGui.QBrush(QtGui.QColor(0, 0, 255))
                item2.setForeground(brush)                              # 2
                self.tableWidget_2.setItem(i, 1 + j * 2, item2)
                self.tableWidget_2.item(i, 1 + j * 2).setTextAlignment(4 |4)         # 4
        self.tableWidget_2.resizeColumnsToContents()   # 7
        self.tableWidget_2.resizeRowsToContents()
    """
        new_item = QtWidgets.QTableWidgetItem('test')
        self.tableWidget.setItem(0,0,new_item)              # 設定某表格內容
        self.tableWidget.item(0,0).text()                   # 表格指定位置的內容
        self.tableWidget.horizontalHeaderItem(0).text()     # 表格第n列的名稱
        self.tableWidget.setHorizontalHeaderLabels()        # 設定表格column 名稱
        self.tableWidget.item(row, column).setToolTip(QString & toolTip)        # 個別item 的提示信息 
        self.tableWidget.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers) # 設表格為唯讀
        self.tableWidget.verticalHeader().setVisible(False)       # 表格row 名稱顯示與否
        self.tableWidget.horizontalHeader().setVisible(False)     # 表格column 名稱顯示與否
        self.tableWidget.setRowHeight(int row, int height)        # 設置指定row 的高度
        self.tableWidget.setColumnWidth(int column, int width)    # 設置指定column 的寬度
        self.tableWidget_2.setAlternatingRowColors(True)    # 隔行交替背景色
    """

    def query_cbl(self):
        """
            查詢特定條件的 基準用電容量(CBL)
        :return:
        """
        if self.spinBox.value() == 0:
            self.show_box(content='參考天數不可為0！')
            return
        if self.spinBox_2.value() == 0:
            self.show_box(content='時間長度不可為0！')
            return
        start_date_time = pd.Timestamp(str(self.dateEdit_2.date().toPyDate() +
                                           pd.offsets.Hour(self.timeEdit.time().hour())))
        end_date_time = start_date_time + pd.offsets.Hour(self.spinBox_2.value())
        self.tz_changed()  # 調整timezone
        if self.radioButton_2.isChecked():
            if self.listWidget.count() == 0:
                self.show_box(content='未指定任何參考日')
                return
            if (self.listWidget.count != 0) & (self.spinBox.value() != self.listWidget.count()):
                self.show_box(content='參考日數量與天數不相符')
                return
        a = pd.Timestamp(str(self.timeEdit.time().toString()))
        b = a + pd.offsets.Hour(self.spinBox_2.value())
        if b.day > a.day:
            self.show_box(content='時間長度不可跨至隔天')
            return

        """ 設定表格
            1. 依CBL 參考天數，設定表格column 數量
            2. 將第2row 的表格全部合併
            3. 將計算好的CBLs指定至特定表格位置，並且將內容置中對齊
            4. 設定column、row 的名稱    
            5. 將計算好的CBL 顯示於第 2 row，並且將內容置中對齊
            6. 將表格的高度、寬度自動依內容調整   
        self.tableWidget.setColumnCount(self.spinBox.value())    # 1
        self.tableWidget.setSpan(1, 0, 1, self.spinBox.value())  # 2
        header_label = list()
        for i in range(len(demands.columns)):
            header_label.append(str(demands.columns[i]))
            item = QtWidgets.QTableWidgetItem(str(round(cbl[i], 3)))  # 3-1
            self.tableWidget.setItem(0, i, item)  # 3-2
            self.tableWidget.item(0, i).setTextAlignment(4 | 4)  # 3-3
        self.tableWidget.setHorizontalHeaderLabels([label for label in header_label])  # 4-1
        self.tableWidget.setVerticalHeaderLabels(['平均值', 'CBL'])  # 4 -2
        item = QtWidgets.QTableWidgetItem(str(round(cbl.mean(), 3)))  #5-1
        self.tableWidget.setItem(1, 0, item)  # 5-2
        self.tableWidget.item(1, 0).setTextAlignment(4 | 4)  # 5-3
        """
        demands = self.calculate_demand(e_date_time=end_date_time)  # DataFrame
        cbl = demands.mean(axis=0, skipna=True)  # Series
        """
            1. 用來設定每一row 有幾個columns
            2. 依cbl 參考日數量設定表格 row、column 的數量
            3. 單數row 顯示日期、偶數row 顯示平均值
            4. 作為cbl 的index
            5. 將每個cell 的內容置中
            6. 將CBL計算結果用藍字呈現
            7. 將表格的高度、寬度自動依內容調整   
        """
        max_column = 5                          # 1
        a = math.ceil(self.spinBox.value()/max_column)
        self.tableWidget.clear()
        self.tableWidget.setColumnCount(max_column)
        self.tableWidget.setRowCount(a*2)       # 2
        for y in range(a):                      # 3
            for x in range(max_column):
                count = x + y * max_column               # 4
                item = QtWidgets.QTableWidgetItem(str(demands.columns[count])) # 日期
                self.tableWidget.setItem(y * 2, x, item)
                self.tableWidget.item(y * 2, x).setTextAlignment(4 | 4)       # 5
                item = QtWidgets.QTableWidgetItem(str(round(cbl[count], 3)))  # 平均值
                self.tableWidget.setItem(y * 2 + 1, x, item)
                self.tableWidget.item(y * 2 + 1, x).setTextAlignment(4 | 4)   # 5
                if count == (self.spinBox.value() - 1):
                    break
        self.label_10.setText(str(round(cbl.mean(),3)))     # 6
        self.label_10.setStyleSheet("color:blue")
        self.tableWidget.resizeColumnsToContents()  # 7
        self.tableWidget.resizeRowsToContents()     # 7
        """
        # 以下是用來摸索調整table widget 大小方式。
        width = self.tableWidget.horizontalHeader().length()    # horizontal 所有cell 的長度
        width += self.tableWidget.frameWidth()*2                # table widget 兩邊框架寬度
        if self.tableWidget.verticalHeader().isVisible():
            width += self.tableWidget.verticalHeader().width()          # row 名稱的寬度
        if self.tableWidget.verticalHeader().isVisible():
            width += self.tableWidget.verticalScrollBar().width()       # 垂直scroller 寬度
        # self.tableWidget.setFixedWidth(width)
        # self.tableWidget.setGeometry(550,590,width,110)
        """

    def calculate_demand(self, e_date_time):
        """
            1. 根據目前時間是否超出取樣時間的最後一段，決定呼叫 define_cbl_date 函式的參數，取得一組list，list 中存有CBL 參考日期
            2. 起始時間為參考日最早的一天，結束時間為參考日最後一天+1
            3. buffer2 的第 0、1 Column 進行相加後乘4的運算，並把結果將 Series的型態存在row_data
        :param e_date_time 傳入的參數數為TimeStamp，為完整的起時和結束的日期+時間
        :return: 將CBL 參考日指定時段的平均需量，用 DataFrame 的方式回傳
        """
        if pd.Timestamp.now() > e_date_time:  # 1
            cbl_date = self.define_cbl_date(e_date_time.date() + pd.offsets.Day(1))
        else:
            cbl_date = self.define_cbl_date(e_date_time.date())
        tags = ['W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11']
        # 2
        buffer2 = query_pi(st=pd.Timestamp(cbl_date[-1]),
                           et=pd.Timestamp(cbl_date[0] + pd.offsets.Day(1)), tags=tags ,extract_type=16)
        row_data = (buffer2.iloc[:, 0] + buffer2.iloc[:, 1]) * 4  # 3
        """
            1. 每天要取樣的起始時間點, 存成list
            2. s_time、e_time 是作為第6點生成一段固定頻率時間的起、終點
            3. 將指定時間長度的需量，一天為一筆(pd.Series 的型態) 儲存至list
            4. 將list 中每筆Series name 更改為日期
            5. 將list 中每筆Series 的index reset
            6. 重新賦予Series 用時間表示的index 
        """
        period_start = [(cbl_date[i] + pd.Timedelta(str(self.timeEdit.time().toPyTime())))
                        for i in range(self.spinBox.value())]       # 1
        # s_time = str(period_start[0].time())        # 2
        # e_time = str((period_start[0] + pd.offsets.Minute((self.spinBox_2.value() * 4 - 1) * 15)).time())

        demands_buffer = list()
        for i in range(self.spinBox.value()):
            s_point = str(period_start[i])
            e_point = str(period_start[i] + pd.offsets.Minute((self.spinBox_2.value() * 4 - 1) * 15))
            demands_buffer.append(row_data.loc[s_point: e_point])                   # 3
            demands_buffer[i].rename(cbl_date[i].date(), inplace=True, copy=False)  # 4
            # demands_buffer[i].reset_index(drop=True, inplace=True)                  # 5
            # demands_buffer[i].index = [a for a in (pd.date_range(s_time, e_time, freq='15min').time)]  # 6
        demands = pd.concat([s for s in demands_buffer], axis=1)

        return demands

    def define_cbl_date(self, date):    #回傳list
        """
        :param date: 此參數數必需是TimeStamp 或 datetime, 用來當作往前找出參考日的起始點
        :return: 將定義好的CBL 參考日以list 的方式回傳
        """
        pending_date = date
        cbl_date = list()
        i = 0
        if self.radioButton.isChecked():            # 找出適當的參考日，並顯示在list widget 中
            self.listWidget.clear()     # 清空list widget
            days = self.spinBox.value()  # 取樣天數
            while i < days:
                pending_date = pending_date - pd.tseries.offsets.BDay(1)
                if self.is_special_date(pending_date):  # 呼叫判斷特殊日的函式
                    continue    # 如果為特殊日，跳過後續流程，再換下一天繼續判斷
                cbl_date.append(pending_date)
                self.listWidget.addItem(str(cbl_date[-1].date()))
                i = i + 1
        else:
            for i in range(self.listWidget.count()):
                cbl_date.append(pd.Timestamp(self.listWidget.item(i).text()))
        return cbl_date

    def is_special_date(self, pending_date):
        """
            用來判斷傳入的日期否，是為特殊日的函式. argument 為待判斷日期
        :param pending_date: 待判斷的日期 (dtype:TimeStamp)
        :return: 用 bool 的方式回傳是或不是
        """
        special_date = pd.concat([self.special_dates.iloc[:,0], self.special_dates.iloc[:,1].dropna()],
                                 axis=0, ignore_index=True)
        for sdate in special_date:      # 將傳進來的日期與special_date 逐一比對，有一樣的就回傳true
            if pending_date == sdate:
                return True
        return False

    def remove_list_item1(self):
        selected = self.listWidget.currentRow() # 取得目前被點撃item 的index
        self.listWidget.takeItem(selected) # 將指定index 的item 刪除

    def add_list_item(self):
        pending_date = pd.Timestamp(self.dateEdit_2.date().toString())
        if pending_date.date() >= pd.Timestamp.today().date():      # datetime格式比較
            self.show_box(content='不可指定今天或未來日期作為CBL參考日期！')
            return
        for i in range(self.listWidget.count()):
            if pending_date == pd.Timestamp(self.listWidget.item(i).text()):
                self.show_box(content='不可重複指定同一天為CBL參考日期！')
                return
        self.listWidget.addItem(str(self.dateEdit_2.date().toPyDate()))  #Add special day to listWidget

    def tz_changed(self):
        self.label_3.setText(self.timeEdit.time().toString())
        self.label_3.setStyleSheet("color:blue")
        lower_limit = pd.Timestamp(self.timeEdit.time().toString()) + pd.offsets.Hour(self.spinBox_2.value())
        self.label_4.setText(str(lower_limit.time()))
        a = pd.Timestamp(str(self.timeEdit.time().toString()))
        b = a + pd.offsets.Hour(self.spinBox_2.value())
        if b.day > a.day:
            self.label_4.setStyleSheet("color:red")
        else:
            self.label_4.setStyleSheet("color:blue")

    def show_box(self, content):
        mbox = QtWidgets.QMessageBox(self)
        mbox.warning(self, '警告', content)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    myWin = MyMainForm()
    myWin.show()
    sys.exit(app.exec())
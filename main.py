import PIconnect as Pi
from PyQt6 import QtCore, QtWidgets, QtGui
import sys, re, time, math, urllib3
import pandas as pd
from PyQt6.QtGui import QLinearGradient
from bs4 import BeautifulSoup
from UI import Ui_Form


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

    # **根據 X 軸進行排序**
    schedule_data.sort(key=lambda x: (x[4], x[0]))  # 先按 process_type 再按 X 座標 排序

    # **去除重複排程 (X 座標過於接近 & 起始時間相同)**
    filtered_schedule = []
    for i in range(len(schedule_data)):
        if i > 0:
            prev_x, prev_start, prev_end, prev_furnace, prev_process = filtered_schedule[-1]
            curr_x, curr_start, curr_end, curr_furnace, curr_process = schedule_data[i]

            # **只有當製程相同時，才檢查 X 座標 & 起始時間是否過於接近**
            if (
                curr_process == prev_process  # **相同製程**
                and abs(curr_x - prev_x) <= 3  # **X 座標過於接近**
                and curr_start == prev_start  # **起始時間相同**
            ):
                # print(f"⚠️ 重複排程移除: {curr_process} {curr_start} ~ {curr_end} (X={curr_x})")
                continue  # **跳過這筆排程，不加入 filtered_schedule**

        filtered_schedule.append(schedule_data[i])

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
        # ---------------統一設定即時值、平均值的背景及文字顏色----------------------
        self.real_time_text = "#145A32"   # 即時量文字顏色 深綠色文字
        self.real_time_back = "#D5F5E3"   # 即時量背景顏色 淡綠色背景
        self.average_text = "#154360"     # 平均值文字顏色 深藍色文字
        self.average_back = "#D6EAF8"     # 平均值背景顏色 淡藍色背景
        #self.real_time_text = "#145A32"  # 即時量文字顏色 深綠色文字
        #self.real_time_back = "#D5F5E3"  # 即時量背景顏色 淡綠色背景
        #self.average_text = "#154360"  # 平均值文字顏色 深藍色文字
        #self.average_back = "#D6EAF8"  # 平均值背景顏色 淡藍色背景

        # self.predict_demand()

        self.tw1.itemExpanded.connect(self.tw1_expanded_event)
        self.tw1.itemCollapsed.connect(self.tw1_expanded_event)
        self.tw3.itemExpanded.connect(self.tw3_expanded_event)
        self.tw3.itemCollapsed.connect(self.tw3_expanded_event)
        self.checkBox.stateChanged.connect(self.check_box_event)
        self.checkBox_2.stateChanged.connect(self.check_box2_event)
        self.query_cbl()      # 查詢特定條件的 基準用電容量(CBL)
        self.query_demand()   # 查詢某一天每一週期的Demand
        self.tws_init()

        self.history_datas_of_groups = pd.DataFrame()  # 用來紀錄整天的各負載分類的週期平均值
        # ------- 關於比對歷史紀錄相關功能的監聽事件、初始狀況及執行設定等 ---------
        self.horizontalScrollBar.valueChanged.connect(self.confirm_value)
        self.dateEdit_3.dateChanged.connect(self.date_edit3_user_change)
        self.checkBox_2.setChecked(False)

        # 使用QThread 的多執行緒，與自動更新選項動作綁定，執行自動更新current value
        self.thread_1 = QtCore.QThread()
        self.thread_1.run = self.continuously_update_current_value
        self.thread_1.start()
        # 使用QThread 的多執行緒，與自動更新選項動作綁定，執行自動更新製程排程
        self.thread_2 = QtCore.QThread()
        self.thread_2.run = self.continuously_scrapy_and_update
        self.thread_2.start()



    def tws_init(self):
        """
        1. 初始化所有treeWidget, tableWidget
        2. 因為treeWidget 的item 文字對齊方式，不知道為何從ui.ui 轉成UI.py 時，預設值都跑掉，所以只能先暫時在這邊設置

        :return:
        """
        # **美化 tw1, tw2, tw3, tw4, tableWidge_3**
        self.beautify_tree_widgets()
        self.beautify_table_widgets()

        # ---------------以下是針對每個treeWidget 設定文字對齊、顏色---------------
        brush2 = QtGui.QBrush(QtGui.QColor(180, 180, 180))  # brush2 用來設定設備群子項的即時量顏色
        brush2.setStyle(QtCore.Qt.BrushStyle.SolidPattern)

        brush3 = QtGui.QBrush(QtGui.QColor(self.real_time_text))  # brush3 用來各一級單位即時量的顏色
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

    def beautify_tree_widgets(self):
        """ 美化 tw1, tw2, tw3 的即時量與平均值欄位，並區分不同表頭顏色 """
        """ 使用 setStyleSheet() 來統一美化 tw1, tw2, tw3,t w4 的表頭 """
        #self.tw1.setStyleSheet("QHeaderView::section { background-color: #c89aa8; color: black; font-weight: bold; }")
        #self.tw2.setStyleSheet("QHeaderView::section { background-color: #c89aa8; color: black; font-weight: bold; }")
        #self.tw3.setStyleSheet("QHeaderView::section { background-color: #f79646; color: black; font-weight: bold; }")
        #self.tw4.setStyleSheet("""
        #    QHeaderView::section {
        #        background-color: #D4AC0D;  /* 金黃色 */
        #        font-size: 16px; /* 與內容一致 */
        #        font-weight: bold;
        #    }
        #""")
        self.tw1.setStyleSheet(
            "QHeaderView::section { background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #52e5e7, stop:1 #130cb7); color: white; font-weight: bold;}")
        self.tw2.setStyleSheet(
            "QHeaderView::section { background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #52e5e7, stop:1 #130cb7); color: white; font-weight: bold;}")
        self.tw3.setStyleSheet(
            "QHeaderView::section { background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #0e6499, stop:1 #9fdeab); color: white; font-weight: bold;}")
        self.tw4.setStyleSheet(
            "QHeaderView::section { background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #fad7a1, stop:1 #e96d71); color: white; font-weight: bold;}")

        column_widths = {
            "tw1": [175, 90, 65],
            "tw2": [175, 90, 65],
            "tw3": [175, 90, 65]
        }

        tree_widgets = {"tw1": self.tw1, "tw2": self.tw2, "tw3": self.tw3}

        for name, widget in tree_widgets.items():
            # 設定 Column 寬度
            widget.setColumnWidth(0, column_widths[name][0])
            widget.setColumnWidth(1, column_widths[name][1])
            widget.setColumnWidth(2, column_widths[name][2])

        # **設定 tw4 column 寬度，確保文字完整顯示**
        self.tw4.setColumnWidth(0, 220)  # **排程時間**
        self.tw4.setColumnWidth(1, 170)  # **狀態**

        # **固定 tw4 column 寬度，防止 tw4.clear() 影響**
        self.tw4.header().setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.Fixed)
        self.tw4.header().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Fixed)

        # **確保 tw4.clear() 不影響 header**
        self.tw4.setHeaderLabels(["製程種類 & 排程時間", "狀態"])

        # **美化tw1,tw2,tw3 即時量 (column 2)**
        for widget in tree_widgets.values():
            for row in range(widget.topLevelItemCount()):
                item = widget.topLevelItem(row)
                item.setFont(1, QtGui.QFont("微軟正黑體", 12))
                item.setBackground(1, QtGui.QBrush(QtGui.QColor("#D5F5E3")))  # 淡綠色背景
                item.setForeground(1, QtGui.QBrush(QtGui.QColor("#145A32")))  # 深綠色文字
                item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

        # **美化平均值 (column 3)**
        for widget in tree_widgets.values():
            for row in range(widget.topLevelItemCount()):
                item = widget.topLevelItem(row)
                item.setFont(2, QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
                item.setBackground(2, QtGui.QBrush(QtGui.QColor("#D6EAF8")))  # 淡藍色背景
                item.setForeground(2, QtGui.QBrush(QtGui.QColor("#154360")))  # 深藍色文字
                item.setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)

        # **針對 tw1 & tw3 (TGs, TG1~TG4) 的即時量，讓它能隨展開事件改變顏色**
        self.tw1.itemExpanded.connect(self.tw1_expanded_event)
        self.tw1.itemCollapsed.connect(self.tw1_expanded_event)
        self.tw3.itemExpanded.connect(self.tw3_expanded_event)
        self.tw3.itemCollapsed.connect(self.tw3_expanded_event)

    def beautify_table_widgets(self):
        """ 使用 setStyleSheet() 統一美化 tableWidget_3 的表頭 """

        # **透過 setStyleSheet() 設定表頭統一風格**
        #self.tableWidget_3.setStyleSheet("QHeaderView::section { background-color: #eff9dd; color: black; font-weight: bold; }")
        self.tableWidget_3.setStyleSheet(
            "QHeaderView::section { background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #FF5D5D, stop:1 #FFB648); color: white; font-weight: bold;}")


        # **設定 Column 寬度**
        column_widths = [90, 100, 65]  # 各欄位的固定寬度
        for i, width in enumerate(column_widths):
            self.tableWidget_3.setColumnWidth(i, width)

        # 設定總類加總 (全廠用電量) 的配色
        #item = self.tableWidget_3.item(0, 0)
        #item.setBackground(QtGui.QBrush(QtGui.QColor("#c89aa8")))
        item = self.tableWidget_3.item(0, 0)
        gradient = QLinearGradient(0,0,1,1)      # 設定比例
        gradient.setCoordinateMode(QLinearGradient.CoordinateMode.ObjectBoundingMode)     # 讓漸層根據 item 大小調整
        gradient.setColorAt(0, QtGui.QColor("#52e5e7"))
        gradient.setColorAt(1, QtGui.QColor("#130cb7"))
        brush = QtGui.QBrush(gradient)
        item.setBackground(brush)       # 設定漸層背景 (與tw1,2 header 相同的漸層配色)
        item.setForeground((QtGui.QBrush(QtGui.QColor('white'))))   # 設定文字顏色為白色

        # 設定總類加總 (中龍發電量) 的配色
        #item = self.tableWidget_3.item(1, 0)
        #item.setBackground(QtGui.QBrush(QtGui.QColor("#c8c688")))
        item = self.tableWidget_3.item(1, 0)
        gradient.setColorAt(0, QtGui.QColor("#0e6499"))
        gradient.setColorAt(1, QtGui.QColor("#9fdeab"))
        brush = QtGui.QBrush(gradient)
        item.setBackground(brush)       # 設定漸層背景 (與tw3 header 相同的漸層配色)
        item.setForeground((QtGui.QBrush(QtGui.QColor('white'))))   # 設定文字顏色為白色

        # **設定欄位樣式，使其與 tw1, tw2, tw3 保持一致**
        for row in range(self.tableWidget_3.rowCount()):
            # 即時量 (column 2)
            item = self.tableWidget_3.item(row, 1)
            if item is None:
                item = QtWidgets.QTableWidgetItem()
                self.tableWidget_3.setItem(row, 1, item)
            item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
            item.setText(item.text())
            item.setBackground(QtGui.QBrush(QtGui.QColor(self.real_time_back)))
            item.setForeground(QtGui.QBrush(QtGui.QColor(self.real_time_text)))

            # 平均值 (column 3)
            item = self.tableWidget_3.item(row, 2)
            if item is None:
                item = QtWidgets.QTableWidgetItem()
                self.tableWidget_3.setItem(row, 2, item)
            item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
            item.setText(item.text())
            item.setBackground(QtGui.QBrush(QtGui.QColor(self.average_back)))
            item.setForeground(QtGui.QBrush(QtGui.QColor(self.average_text)))

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
            tw2_width = self.tw2.columnWidth(0) + self.tw2.columnWidth(1) + self.tw2.columnWidth(2) + 20
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
            tw2_width = self.tw2.columnWidth(0) + self.tw2.columnWidth(1) + 20
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
        self.label_23.setText(str(f'%s MW' %(self.predict_demand())))

        # self.update_tw4_schedule()

    def update_tw4_schedule(self):
        """
        更新 tw4 (treeWidget) 顯示 scrapy_schedule() 解析的排程資訊：
        - 第一層：製程種類 (EAF, LF1-1, LF1-2)
        - 第二層："生產或等待中" (current + future) / "過去排程" (past)
        - 若無 "生產或等待中" 排程，仍增加此分類，但不增加子排程，並顯示 "目前無排程"
        - 若無 "過去排程" 資料，仍增加此分類，但不增加子排程，並顯示 "無相關排程"
        - **column 2 (狀態欄) 文字置中**
        """
        past_df, current_df, future_df = scrapy_schedule()
        self.tw4.clear()

        process_map = {"EAF": None, "LF1-1": None, "LF1-2": None}

        for process_name in process_map.keys():
            process_parent = QtWidgets.QTreeWidgetItem(self.tw4)
            process_parent.setText(0, process_name)
            self.tw4.addTopLevelItem(process_parent)

            # **過濾當前製程的排程**
            active_schedules = pd.concat([
                current_df.assign(類別="current"),
                future_df.assign(類別="future")
            ], ignore_index=True).sort_values(by="開始時間")
            active_schedules = active_schedules[
                (active_schedules["製程"] == process_name) |
                ((process_name == "EAF") & active_schedules["製程"].isin(["EAFA", "EAFB"]))
                ]

            past_schedules = past_df[
                (past_df["製程"] == process_name) |
                ((process_name == "EAF") & past_df["製程"].isin(["EAFA", "EAFB"]))
                ].sort_values(by="開始時間")

            # **處理 "生產或等待中"**
            active_parent = QtWidgets.QTreeWidgetItem(process_parent)
            active_parent.setText(0, "生產或等待中")
            process_parent.addChild(active_parent)

            if not active_schedules.empty:
                for _, row in active_schedules.iterrows():
                    start_time = row["開始時間"].strftime("%H:%M:%S")
                    end_time = row["結束時間"].strftime("%H:%M:%S")
                    category = row["類別"]
                    status = str(row["製程狀態"]) if "製程狀態" in row and pd.notna(row["製程狀態"]) else "N/A"

                    if row["製程"] == "EAFA":
                        process_display = "EAF"
                        status += " (A爐)"
                    elif row["製程"] == "EAFB":
                        process_display = "EAF"
                        status += " (B爐)"
                    else:
                        process_display = row["製程"]

                    if process_display != process_name:
                        continue

                    item = QtWidgets.QTreeWidgetItem(active_parent)
                    item.setText(0, f"{start_time} ~ {end_time}")
                    item.setText(1, status)

                    # **狀態欄 (column 2) 文字置中**
                    item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

                    if category == "current":
                        item.setBackground(0, QtGui.QBrush(QtGui.QColor("#FCF8BC")))  # **淡黃色背景**
                        item.setBackground(1, QtGui.QBrush(QtGui.QColor("#FCF8BC")))
                    elif category == "future":
                        minutes = int((row["開始時間"] - pd.Timestamp.now()).total_seconds() / 60)
                        item.setText(1, f"{minutes} 分鐘後開始生產")
                        item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)  # **未來排程置中**

                    active_parent.addChild(item)

            else:
                # **若無生產或等待中排程，在 column 2 顯示 "目前無排程"，並置中**
                active_parent.setText(1, "目前無排程")
                active_parent.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

            # **處理 "過去排程"**
            past_parent = QtWidgets.QTreeWidgetItem(process_parent)
            past_parent.setText(0, "過去排程")
            process_parent.addChild(past_parent)

            if not past_schedules.empty:
                for _, row in past_schedules.iterrows():
                    start_time = row["開始時間"].strftime("%H:%M:%S")
                    end_time = row["結束時間"].strftime("%H:%M:%S")

                    item = QtWidgets.QTreeWidgetItem(past_parent)
                    item.setText(0, f"{start_time} ~ {end_time}")
                    item.setText(1, "已完成")
                    item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)  # **過去排程置中**

                    past_parent.addChild(item)

            else:
                # **若無過去排程，在 column 2 顯示 "無相關排程"，並置中**
                past_parent.setText(1, "無相關排程")
                past_parent.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

        # **確保所有節點展開**
        self.tw4.expandAll()  # ✅ 確保所有製程展開

    def predict_demand(self):
        """
        1. 計算預測的demand。目前預測需量的計算方式為，
        目前週期的累計需量值 + 近180秒的平均需量 / 180 x 該剩期剩餘秒數
        :return:
        """
        st = pd.Timestamp.now().floor('15T')    # 目前週期的起始時間
        et = st + pd.offsets.Minute(15)         # 目前週期的結束時間

        back_150s_from_now = pd.Timestamp.now() - pd.offsets.Second(180)    # 150秒前的時間點
        diff_between_now_and_et = (et - pd.Timestamp.now()).total_seconds()   # 此週期剩餘時間

        tags = self.tag_list.loc[0:1,'tag_name2']
        tags.index = self.tag_list.loc[0:1,'name']
        name_list = tags.loc[:].values.tolist()

        # 查詢目前週期的累計需量值
        query_result = query_pi(st=st, et=et, tags=name_list ,extract_type = 16)

        # 將資料型態從Object -> float，若有資料中有文字無法換的，則用NaN 缺失值取代。
        query_result.iloc[0,:] = pd.to_numeric(query_result.iloc[0,:], errors='coerce')
        current_accumulation = query_result.sum(axis = 1) * 4

        # 查近180秒的平均需量，並計算出剩餘時間可能會增加的需量累計值
        result = query_pi(st=back_150s_from_now, et=back_150s_from_now + pd.offsets.Second(180),
                             tags=name_list ,extract_type = 16)
        weight2 = 4 / 180 * diff_between_now_and_et

        # 將資料型態從Object -> float，若有資料中有文字無法換的，則用NaN 缺失值取代。
        result.iloc[0,:] = pd.to_numeric(result.iloc[0,:], errors='coerce')
        predict = result.sum(axis=1) * weight2

        # 取四捨五入
        demand = round((current_accumulation[0] + predict[0]),2)
        return demand

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
            # ------選擇當天日期時，查詢完資料後，顯示前一個週期的資料，其它日期則顯示第一個週期的資料
            sd = pd.Timestamp(self.dateEdit_3.date().toString())
            ed = sd + pd.offsets.Day(1)
            self.history_demand_of_groups(st=sd, et=ed)
            if pd.Timestamp(self.dateEdit_3.date().toString()).normalize() == pd.Timestamp.today().normalize():
                sp = pd.Timestamp.now().floor('15T')
                self.horizontalScrollBar.setValue((sp - pd.Timestamp.now().normalize()) // pd.Timedelta('15T') - 1)
            else:
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

        self.update_table_item(0, 2, pre_check2(tai_power), self.average_back, self.average_text, bold=True)
        self.update_table_item(1, 2, pre_check2(current_p['2H120':'5KB19'].sum()), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(2, 2, pre_check2(sun_power, b=5), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(3, 2, pre_check2(current_p['feeder 1510':'feeder 1520'].sum(), b=4), self.average_back,
                               self.average_text, bold=True)

    def tws_update(self, current_p):
        """
        更新樹狀結構(tree widget)、表格結構(table widget) 裡的資料
        :param current_p: 即時用電量。pd.Series
        :return:
        """
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

        self.tw2.topLevelItem(0).setText(1, pre_check(current_p['9H140':'9KB33'].sum(), 0))
        self.tw2.topLevelItem(1).setText(1, pre_check(current_p['AH120'], 0))
        self.tw2.topLevelItem(2).setText(1, pre_check(current_p['AH190'], 0))
        self.tw2.topLevelItem(3).setText(1, pre_check(current_p['AH130'],0))
        self.tw2.topLevelItem(4).setText(1, pre_check(current_p['1H360'], 0))
        self.tw2.topLevelItem(5).setText(1, pre_check(current_p['1H450'], 0))

        ng_to_power = self.unit_prices.loc['可轉換電力', 'current']

        self.tw3.topLevelItem(0).setText(1, pre_check(current_p['2H120':'1H420'].sum()))
        self.tw3.topLevelItem(0).child(0).setText(1, pre_check(current_p['2H120':'2H220'].sum()))
        self.tw3.topLevelItem(0).child(1).setText(1, pre_check(current_p['5H120':'5H220'].sum()))
        self.tw3.topLevelItem(0).child(2).setText(1, pre_check(current_p['1H120':'1H220'].sum()))
        self.tw3.topLevelItem(0).child(3).setText(1, pre_check(current_p['1H320':'1H420'].sum()))
        self.tw3.topLevelItem(1).setText(1, pre_check(current_p['4KA18':'5KB19'].sum()))
        self.tw3.topLevelItem(1).child(0).setText(1, pre_check(current_p['4KA18']))
        self.tw3.topLevelItem(1).child(1).setText(1, pre_check(current_p['5KB19']))
        self.tw3.topLevelItem(2).setText(1, pre_check(current_p['4H120':'4H220'].sum()))
        self.tw3.topLevelItem(2).child(0).setText(1, pre_check(current_p['4H120']))
        self.tw3.topLevelItem(2).child(1).setText(1, pre_check(current_p['4H220']))

        # tw3 的TGs 及其子節點 TG1~TG4 的 NG貢獻電量、使用量，從原本顯示在最後兩個column，改為顯示在3rd 的tip
        ng = pd.Series([current_p['TG1 NG':'TG4 NG'].sum(), current_p['TG1 NG'], current_p['TG2 NG'],
                        current_p['TG3 NG'], current_p['TG4 NG'], ng_to_power])
        self.update_tw3_tips_and_colors(ng)

        # 方式 2：table widget 3 利用 self.update_table_item 函式，在更新內容後，保留原本樣式不變
        tai_power = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
                    - current_p['sp_real_time']

        self.update_table_item(0, 1, pre_check(tai_power), self.real_time_back, self.real_time_text)
        self.update_table_item(1, 1, pre_check(current_p['2H120':'5KB19'].sum()), self.real_time_back, self.real_time_text)  # 即時量
        self.update_table_item(2, 1, pre_check(current_p['sp_real_time'], b=5), self.real_time_back, self.real_time_text)
        self.update_table_item(3, 1, pre_check(current_p['feeder 1510':'feeder 1520'].sum(), b=4), self.real_time_back, self.real_time_text)

    def update_table_item(self, row, column, text, background_color, text_color, bold=False):
        """
        更新 tableWidget_3 的數據，並確保樣式不變
        """
        item = self.tableWidget_3.item(row, column)
        if item is None:
            item = QtWidgets.QTableWidgetItem()
            self.tableWidget_3.setItem(row, column, item)

        item.setText(text)
        item.setBackground(QtGui.QBrush(QtGui.QColor(background_color)))
        item.setForeground(QtGui.QBrush(QtGui.QColor(text_color)))

        # 設定微軟正黑體，平均值 (column 3) 需要加粗
        font = QtGui.QFont('微軟正黑體', 12)
        if bold:
            font.setBold(True)
        item.setFont(font)

        item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

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
            <b>NG 流量:</b> <span style="color:#0000FF;">{ng[0]:.2f} Nm³/hr</span><br>
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
                <b>NG 流量:</b> <span style="color:#0000FF;">{ng_usage:.2f} Nm³/hr</span><br>
                <b>NG 貢獻電量:</b> <span style="color:#FF0000;">{ng_contribution:.2f} MW</span>
            </div>
            """
            tg_child.setToolTip(1, tooltip_text)  # 針對 2nd column (即時量) 設定美化 Tooltip

            # 變更字體顏色
            tg_child.setForeground(1, QtGui.QBrush(highlight_color if ng_contribution > 0 else default_color))

    def continuously_update_current_value(self):
        """
        用來每隔11秒，自動更新current value
        :return:
        """
        while True:
            self.dashboard_value()
            time.sleep(11)

    def continuously_scrapy_and_update(self):
        """
        用來每隔30秒，自動更新爬製程排程相關資訊
        :return:
        """
        while True:
            self.update_tw4_schedule()
            time.sleep(30)

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
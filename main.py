import PIconnect as Pi
from PyQt6 import QtCore, QtWidgets, QtGui
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
    :param pending_data:要判斷的數值。
    :param b:若數值接近 0，預設回傳'停機'的述述。
    :return: 回傳值為文字型態。
    """
    describe = ['未生產', '停機', '資料異常','未使用','0 MW','未發電']
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
    :param pending_data:
    :return:
    """
    describe = ['未生產', '停機', '資料異常', '未使用', '0 MW', '未發電']
    if pd.isnull(pending_data):
        return describe[2]
    if pending_data > 0.1:
        return str(format(round(pending_data, 2), '.2f'))
    else:
        return describe[b]

def scrapy_schedule():
    """
    爬取"製程管理資訊 2138"，從中解析出電爐的製程。
    :return: 下一爐時間
    """
    t_count = 0
    f_count = 0
    p_count = 0
    past = list()
    future = list()
    quote_page = 'http://w3mes.dscsc.dragonsteel.com.tw/2138.aspx'
    http = urllib3.PoolManager()

    r = http.request('GET', quote_page)  # 透過HTTP 請求從"製程管理資訊 2138"獲取網頁
    soup = BeautifulSoup(r.data, 'html.parser')  # 用BS 的html.parer 解析該網頁
    contains = soup.find_all('area')  # 尋找內容裡所有名稱叫做area 的tag (圖像地圖區域元素)
    for contain in contains:
        if 'EAF' in contain.get('title'):  # 找出含有EAF 的title
            coords = re.findall(r"\d+", contain.get('coords'))  # 提取出 title 的座標
            # \d+ 是一個正則表達示，意思是"一個或多個數字(0-9)
            if (int(coords[1]) > 182) & (int(coords[1]) < 235):  # 利用 title 左上角的y軸座標,判斷title 的內容要不要提取
                t_count = t_count + 1
                if '送電' in contain.get('title'):  # 已結束EAF 製程的部份(特徵為內容中有'送電'這個詞)
                    p_count = p_count + 1
                    pending_str = contain.get('title')  # 從contain 中獲取 title 的內容
                    start = pd.to_datetime(pending_str[pending_str.find(':') + 2: pending_str.find(':') + 7])
                    end = pd.to_datetime(pending_str[pending_str.find(':') + 10: pending_str.find(':') + 15])
                    if start > end:  # 若end time 比star time早,確認是跨天,end time +1 day
                        end = end + pd.offsets.Day(1)
                    result = pd.Series([start, end])
                    past.append(result)

                if '時間' in contain.get('title'):  # 還未完成EAF 製程的部份 (特徵為第一個找到的'時間'這個詞)
                    pending_str = contain.get('title')  # 從contain 中獲取 title 的內容
                    start = pd.to_datetime(pending_str[pending_str.find(':') + 2: pending_str.find(':') + 10])
                    end = pd.to_datetime(pending_str[pending_str.find(':') + 13: pending_str.find(':') + 21])
                    if start > end:  # 若end time 比star time早, 確認是跨天 (end time +1 day)
                        end = end + pd.offsets.Day(1)
                    if start > pd.Timestamp.now():  # 用來過濾掉非未來排程的部份
                        f_count = f_count + 1
                        result = pd.Series([start, end])
                        future.append(result)
                    elif (start < pd.Timestamp.now()) and (pd.Timestamp.now() < end):  # 正在執行的排程
                        current = pd.Series([start, end])
                    else:
                        t_count = t_count - 1
    if f_count > 1:
        future = sorted(future, key=lambda x: x[0])
    return future

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
        self.tw3.itemExpanded.connect(lambda item: self.tw3_expanded_event(item))
        self.tw3.itemCollapsed.connect(lambda item: self.tw3_expanded_event(item))
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

        # -------關於 水平scroller 的初始設定及執行---------
        # 追蹤是否正在拖動滑塊
        self.dragging = False
        # 監聽 horizontalScrollBar 事件
        self.horizontalScrollBar.sliderPressed.connect(self.start_dragging)   # 按下滑塊開始拖動
        self.horizontalScrollBar.sliderMoved.connect(self.preview_value)      # 滑鼠拖動時只更新暫存數值
        self.horizontalScrollBar.sliderReleased.connect(self.confirm_value)   # 滑鼠放開時才更新正式數值
        self.horizontalScrollBar.actionTriggered.connect(self.handle_action)  # 點擊箭頭或空白區時，允許即時更新
        self.dateEdit_3.userDateChanged.connect(self.confirm_value)
        self.checkBox_2.setChecked(False)

    def start_dragging(self):
        """當使用者按住滑塊時，標記為拖動狀態"""
        self.dragging = True

    def preview_value(self, value):
        """當滑鼠拖動時，只更新暫存數值，不更新正式數值"""
        et = pd.Timestamp(self.dateEdit_3.date().toString()) + pd.offsets.Minute(15) * value
        st = et - pd.offsets.Minute(15)
        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))

    def handle_action(self, action):
        """當數值變更時，根據是否正在拖動來決定是否更新正式數值"""
        if not self.dragging:  # 如果不是拖動狀態，則允許正式數值更新
             self.confirm_value()

    def confirm_value(self):
        """當滑鼠放開或點擊滾動條結束後，才正式更新數值"""
        self.label_22.setText('')
        self.dragging = False   # 解除拖動狀態
        et = pd.Timestamp(self.dateEdit_3.date().toString()) + pd.offsets.Minute(15) * self.horizontalScrollBar.value()
        if et > pd.Timestamp.now():     # 欲查詢的時間段，屬於未來時間時
            # 將et 設定在最接近目前時間點之前的最後15分鐘結束點, 並將 scrollerBar 調整至相對應的值
            et = pd.Timestamp.now().floor('15T')
            self.horizontalScrollBar.setValue((et - pd.Timestamp.now().normalize()) // pd.Timedelta('15T'))
            self.label_22.setText('只能比對歷史紀錄！')
        st = et - pd.offsets.Minute(15)

        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))
        # 防止scroller 的值在最高時，日期值會更新到隔天，而引發不可預知的錯誤
        if self.horizontalScrollBar.value() < 96:
            self.dateEdit_3.setDate(QtCore.QDate(et.year,et.month,et.day))
        else:
            self.dateEdit_3.setDate(QtCore.QDate(st.year, st.month, st.day))
        self.history_of_groups_demand(st=st, et=et)

    def check_box2_event(self):
        et = pd.Timestamp.now().floor('15T')
        st = et - pd.offsets.Minute(15)
        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))

        # 防止scroller 的值在最高時，日期值會更新到隔天，而引發不可預知的錯誤
        if self.horizontalScrollBar.value() < 96:
            self.dateEdit_3.setDate(QtCore.QDate(et.year, et.month, et.day))
            if st.date() < et.date():       # 當天第一個週期時，dateEdit 會預設在昨日
                self.dateEdit_3.setDate(QtCore.QDate(st.year, st.month, st.day))
        else:
            self.dateEdit_3.setDate(QtCore.QDate(st.year, st.month, st.day))


        if self.checkBox_2.isChecked():
            self.history_of_groups_demand(st=st, et=et)
            #------function visible_____
            self.dateEdit_3.setVisible(True)
            self.horizontalScrollBar.setVisible(True)
            self.label_16.setVisible(True)
            self.label_17.setVisible(True)
            self.label_19.setVisible(True)
            self.label_21.setVisible(True)
            self.label_22.setVisible(True)
            #----------------------tree widget----------------
            self.tw1.setGeometry(QtCore.QRect(9, 10, 374, 191))  # scroller width 18
            self.tw1.setColumnWidth(0, 175)  # 設定各column 的寬度
            self.tw1.setColumnWidth(1, 90)
            self.tw1.setColumnWidth(2, 90)
            self.tw1.setColumnHidden(2, False)
            self.tw2.setGeometry(QtCore.QRect(410, 10, 334, 191))
            self.tw2.setColumnWidth(0, 135)  # 設定各column 的寬度
            self.tw2.setColumnWidth(1, 90)
            self.tw2.setColumnWidth(2, 90)
            self.tw2.setColumnHidden(2, False)
            self.tw3.setGeometry(QtCore.QRect(300, 460, 530, 141))
            self.tw3.setColumnHidden(2, False)
            self.tableWidget_3.setGeometry(QtCore.QRect(10, 250, 301, 151))
            self.tableWidget_3.setColumnHidden(2, False)
        else:
            # ------function visible_____
            self.dateEdit_3.setVisible(False)
            self.horizontalScrollBar.setVisible(False)
            self.label_16.setVisible(False)
            self.label_17.setVisible(False)
            self.label_19.setVisible(False)
            self.label_21.setVisible(False)
            self.label_22.setVisible(False)
            # ----------------------tree widget----------------
            self.tw1.setGeometry(QtCore.QRect(9, 10, 284, 191))
            self.tw1.setColumnWidth(0, 175)  # 設定各column 的寬度
            self.tw1.setColumnWidth(1, 90)
            self.tw1.setColumnWidth(2, 90)
            self.tw1.setColumnHidden(2, True)
            self.tw2.setGeometry(QtCore.QRect(410, 10, 227, 191))
            self.tw2.setColumnWidth(0, 135)  # 設定各column 的寬度
            self.tw2.setColumnWidth(1, 90)
            self.tw2.setColumnWidth(2, 100)
            self.tw2.setColumnHidden(2, True)
            self.tw3.setGeometry(QtCore.QRect(300, 460, 430, 141))
            self.tw3.setColumnHidden(2, True)
            self.tableWidget_3.setGeometry(QtCore.QRect(10, 250, 201, 151))
            self.tableWidget_3.setColumnHidden(2, True)
    # @timeit
    def history_of_groups_demand(self, st, et):
        """
            查詢特定週期，各設備群組(分類)的平均值

        :return:
        """
        mask = ~pd.isnull(self.tag_list.loc[:,'tag_name2'])     # 作為用來篩選出tag中含有有kwh11 的布林索引器
        groups_demand = self.tag_list.loc[mask, 'tag_name2':'Group2']
        groups_demand.index = self.tag_list.loc[mask,'name']
        name_list = groups_demand.loc[:,'tag_name2'].values.tolist() # 把DataFrame 中標籤名為tag_name2 的值，轉成list輸出
        query_result = query_pi(st=st, et=et, tags=name_list ,extract_type = 16)

        groups_demand.loc[:, 'demand'] = query_result.T.values  # 把結果轉置後，複制並新增到到groups_demand 的最後一個column
        groups_demand.loc[:, 'demand'] = pd.to_numeric(groups_demand.loc[:, 'demand'], errors='coerce')  # 轉換資料型態 object->float，若遇文字型態，則用Nan 取代。
        groups_demand.loc[:, 'demand'] = groups_demand.loc[:, 'demand'] * 4         # kwh -> MW/15 min
        wx_grouped = groups_demand.groupby(['Group1','Group2'])['demand'].sum()     # 利用 group by 的功能，依Group1(單位)、Group2(負載類型)進行分組，將分組結果套入sum()的方法
        wx = pd.DataFrame(wx_grouped.loc['W2':'WA', 'B'])
        wx.index = wx.index.get_level_values(0)             # 重新將index 設置為原multiIndex 的第一層index 內容
        groups_demand = pd.concat([groups_demand, wx],axis=0) # 將wx 內容新增到group_demand 之後。

        self.update_history_to_tws(groups_demand['demand'])

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

        brush1 = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # 給白色文字用的設定
        brush1.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush2 = QtGui.QBrush(QtGui.QColor(80, 191, 200))  # 全廠用電量的背景色
        brush2.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush3 = QtGui.QBrush(QtGui.QColor(100, 170, 90))  # 中龍發電量的背景色
        brush3.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush4 = QtGui.QBrush(QtGui.QColor(170, 170, 0))  # 中龍發電量的背景色
        brush4.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush5 = QtGui.QBrush(QtGui.QColor(190, 90, 90))  # 中龍發電量的背景色
        brush5.setStyle(QtCore.Qt.BrushStyle.SolidPattern)

        font = QtGui.QFont()
        font.setFamily("微軟正黑體")
        font.setPointSize(12)
        font.setBold(True)

        #taipower = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
        #            - current_p['sp_real_time']
        sun_power = current_p['9KB25-4_2':'3KA12-1_2'].sum()
        taipower = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() - sun_power

        item01 = QtWidgets.QTableWidgetItem(pre_check2(taipower))
        item01.setForeground(brush1)
        item01.setBackground(brush2)
        item01.setFont(font)
        item01.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(0, 2, item01)

        item11 = QtWidgets.QTableWidgetItem(pre_check2(current_p['2H120':'5KB19'].sum()))
        item11.setForeground(brush1)
        item11.setBackground(brush3)
        item11.setFont(font)
        item11.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(1, 2, item11)

        item21 = QtWidgets.QTableWidgetItem(pre_check2(sun_power,b=5))
        item21.setForeground(brush1)
        item21.setBackground(brush4)
        item21.setFont(font)
        item21.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(2, 2, item21)
                                           #pre_check(current_p['TG1 NG'], 3, 'gas'))
        item31 = QtWidgets.QTableWidgetItem(pre_check2(current_p['feeder 1510':'feeder 1520'].sum()))
        item31.setForeground(brush1)
        item31.setBackground(brush5)
        item31.setFont(font)
        item31.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(3, 2, item31)
        
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

    def tw3_expanded_event(self,item):
        """
        1. 用來同步TGs 發電量、NG貢獻電量、NG使用量的項目展開、收縮
        2. 所有項目在expanded 或 collapsed 時，變更文字顯示的方式
        :param item: tw3 發生expanded 或 collased 的子項目
        :return:
        """
        if item.text(0) == 'TGs':
            if item.isExpanded():
                self.tw4.topLevelItem(0).setExpanded(True)
            else:
                self.tw4.topLevelItem(0).setExpanded(False)

        b_transparent = QtGui.QBrush(QtGui.QColor(0,0,0,0))
        b_solid  = QtGui.QBrush(QtGui.QColor(0,0,0, 255))
        # TGs
        if self.tw3.topLevelItem(0).isExpanded():
            self.tw3.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(0).setForeground(1, b_transparent)
            # tw3 擴增、tw4刪除
            self.tw3.topLevelItem(0).setForeground(2, b_transparent)
            self.tw3.topLevelItem(0).setForeground(3, b_transparent)
            # self.tw3.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw3.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(0).setForeground(1, b_solid)
            # tw3 擴增、tw4刪除
            self.tw3.topLevelItem(0).setForeground(2, b_solid)
            self.tw3.topLevelItem(0).setForeground(3, b_solid)
            # self.tw3.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        # TRTs
        if self.tw3.topLevelItem(1).isExpanded():
            self.tw3.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(1).setForeground(1, b_transparent)
            # self.tw3.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw3.topLevelItem(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(1).setForeground(1, b_solid)
            # self.tw3.topLevelItem(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        # CDQs
        if self.tw3.topLevelItem(2).isExpanded():
            self.tw3.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            self.tw3.topLevelItem(2).setForeground(1, b_transparent)
            # self.tw3.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw3.topLevelItem(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tw3.topLevelItem(2).setForeground(1, b_solid)
            # self.tw3.topLevelItem(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        # NGs
        if self.tw4.topLevelItem(0).isExpanded():
            self.tw4.topLevelItem(0).setForeground(0, b_transparent)
            self.tw4.topLevelItem(0).setForeground(1, b_transparent)
            # self.tw4.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
            # self.tw4.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
        else:
            self.tw4.topLevelItem(0).setForeground(0, b_solid)
            self.tw4.topLevelItem(0).setForeground(1, b_solid)
            # self.tw4.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
            # self.tw4.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

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

    def tws_init(self):
        """
        1. 因為treeWidget 的item 文字對齊方式，不知道為何從ui.ui 轉成UI.py 時，預設值都跑掉，所以只能先暫時在這邊設置
        :return:
        """
        self.tw1.setStyleSheet("QHeaderView::section{background:rgb(85, 181, 200);}")  # 設置表頭的背景顏色
        brush = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # brush 用來設定顏色種類
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)  # 設定顏色的分佈方式
        self.tw1.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw1.headerItem().setForeground(1, brush)
        self.tw1.headerItem().setForeground(2, brush)
        self.tw1.setGeometry(QtCore.QRect(9, 10, 374, 191))     #scroller width 18, frame line width 1
        self.tw1.setColumnWidth(0, 175)  # 設定各column 的寬度
        self.tw1.setColumnWidth(1, 90)
        self.tw1.setColumnWidth(2, 90)

        #self.tw1.setColumnHidden(2,True)

        self.tw2.setStyleSheet("QHeaderView::section{background:rgb(85, 181, 200);}")  # 設置表頭的背景顏色
        self.tw2.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw2.headerItem().setForeground(1, brush)
        self.tw2.headerItem().setForeground(2, brush)
        self.tw2.setGeometry(QtCore.QRect(410, 10, 334, 191))
        self.tw2.setColumnWidth(0, 135)     # 設定各column 的寬度
        self.tw2.setColumnWidth(1, 90)
        self.tw2.setColumnWidth(2, 90)
        #self.tw1.setColumnHidden(2,True)

        self.tw3.setStyleSheet("QHeaderView::section{background:rgb(100, 170, 90);}")  # 設置表頭的背景顏色
        brush = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # brush 用來設定顏色種類
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)  # 設定顏色的分佈方式
        self.tw3.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw3.headerItem().setForeground(1, brush)
        self.tw3.headerItem().setForeground(2, brush)
        self.tw3.headerItem().setForeground(3, brush)
        self.tw3.setGeometry(QtCore.QRect(300, 460, 530, 141))
        self.tw3.setColumnWidth(0, 110)  # tw3 total width: 221
        self.tw3.setColumnWidth(1, 100)
        self.tw3.setColumnWidth(2, 100)
        self.tw3.setColumnWidth(3, 100)
        self.tw3.setColumnWidth(4, 100)

        self.tw4.setStyleSheet("QHeaderView::section{background:rgb(100, 170, 90);}")  # 設置表頭的背景顏色
        brush = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # brush 用來設定顏色種類
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)  # 設定顏色的分佈方式
        self.tw4.headerItem().setForeground(0, brush)  # 設置表頭項目的字體顏色
        self.tw4.headerItem().setForeground(1, brush)
        self.tw4.setColumnWidth(0, 125)

        self.tableWidget_3.setGeometry(QtCore.QRect(10, 250, 201, 151))
        """
        # self.treeWidget.hideColumn(0) # 用來隱藏指定的column
        # self.treeWidget.clear()       # clean all data
        """
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
        self.tw3.topLevelItem(0).setTextAlignment(3, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).setTextAlignment(4, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(3, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(3, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(3, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(3, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(0).setTextAlignment(4, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(1).setTextAlignment(4, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(2).setTextAlignment(4, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw3.topLevelItem(0).child(3).setTextAlignment(4, QtCore.Qt.AlignmentFlag.AlignRight)

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
        """
        tw3 擴增、tw4刪除
        self.tw4.topLevelItem(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(0).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(0).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(1).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(1).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(2).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(2).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(3).setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignRight)
        self.tw4.topLevelItem(0).child(3).setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)   
        """

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
        # tw3 擴增、tw4刪除
        self.tw3.topLevelItem(0).setText(3, pre_check(current_p['TG1 NG':'TG4 NG'].sum() * ng_to_power /1000, 4))
        self.tw3.topLevelItem(0).child(0).setText(3, pre_check(current_p['TG1 NG'] * ng_to_power / 1000, 4))
        self.tw3.topLevelItem(0).child(1).setText(3, pre_check(current_p['TG2 NG'] * ng_to_power / 1000, 4))
        self.tw3.topLevelItem(0).child(2).setText(3, pre_check(current_p['TG3 NG'] * ng_to_power / 1000, 4))
        self.tw3.topLevelItem(0).child(3).setText(3, pre_check(current_p['TG4 NG'] * ng_to_power / 1000, 4))
        self.tw3.topLevelItem(0).setText(4, pre_check(current_p['TG1 NG':'TG4 NG'].sum(), 3, 'gas'))
        self.tw3.topLevelItem(0).child(0).setText(4, pre_check(current_p['TG1 NG'], 3, 'gas'))
        self.tw3.topLevelItem(0).child(1).setText(4, pre_check(current_p['TG2 NG'], 3, 'gas'))
        self.tw3.topLevelItem(0).child(2).setText(4, pre_check(current_p['TG3 NG'], 3, 'gas'))
        self.tw3.topLevelItem(0).child(3).setText(4, pre_check(current_p['TG4 NG'], 3, 'gas'))

        self.tw3.topLevelItem(1).setText(1, pre_check(current_p['4KA18':'5KB19'].sum()))
        self.tw3.topLevelItem(1).child(0).setText(1, pre_check(current_p['4KA18']))
        self.tw3.topLevelItem(1).child(1).setText(1, pre_check(current_p['5KB19']))
        self.tw3.topLevelItem(2).setText(1, pre_check(current_p['4H120':'4H220'].sum()))
        self.tw3.topLevelItem(2).child(0).setText(1, pre_check(current_p['4H120']))
        self.tw3.topLevelItem(2).child(1).setText(1, pre_check(current_p['4H220']))
        """
        tw3 擴增、tw4刪除
        self.tw4.topLevelItem(0).setText(0, pre_check(current_p['TG1 NG':'TG4 NG'].sum() * ng_to_power /1000, 4))
        self.tw4.topLevelItem(0).setText(1, pre_check(current_p['TG1 NG':'TG4 NG'].sum(), 3, 'gas'))
        self.tw4.topLevelItem(0).child(0).setText(0, pre_check(current_p['TG1 NG'] * ng_to_power / 1000, 4))
        self.tw4.topLevelItem(0).child(0).setText(1, pre_check(current_p['TG1 NG'], 3, 'gas'))
        self.tw4.topLevelItem(0).child(1).setText(0, pre_check(current_p['TG2 NG'] * ng_to_power / 1000, 4))
        self.tw4.topLevelItem(0).child(1).setText(1, pre_check(current_p['TG2 NG'], 3, 'gas'))
        self.tw4.topLevelItem(0).child(2).setText(0, pre_check(current_p['TG3 NG'] * ng_to_power / 1000, 4))
        self.tw4.topLevelItem(0).child(2).setText(1, pre_check(current_p['TG3 NG'], 3, 'gas'))
        self.tw4.topLevelItem(0).child(3).setText(0, pre_check(current_p['TG4 NG'] * ng_to_power / 1000, 4))
        self.tw4.topLevelItem(0).child(3).setText(1, pre_check(current_p['TG4 NG'], 3, 'gas'))
        
        self.tw4.topLevelItem(1).setText(1, pre_check(current_p['4KA18'], 3))
        self.tw4.topLevelItem(1).setText(1, pre_check(current_p['5KB19'], 3))
        self.tw4.topLevelItem(2).setText(1, pre_check(current_p['4H120'],3))
        self.tw4.topLevelItem(3).setText(1, pre_check(current_p['4H220'],3))
        """
        brush1 = QtGui.QBrush(QtGui.QColor(255, 255, 255))  # 給白色文字用的設定
        brush1.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush2 = QtGui.QBrush(QtGui.QColor(80, 191, 200))  # 全廠用電量的背景色
        brush2.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush3 = QtGui.QBrush(QtGui.QColor(100, 170, 90))  # 中龍發電量的背景色
        brush3.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush4 = QtGui.QBrush(QtGui.QColor(170, 170, 0))  # 中龍發電量的背景色
        brush4.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        brush5 = QtGui.QBrush(QtGui.QColor(190, 90, 90))  # 中龍發電量的背景色
        brush5.setStyle(QtCore.Qt.BrushStyle.SolidPattern)

        font = QtGui.QFont()
        font.setFamily("微軟正黑體")
        font.setPointSize(12)
        font.setBold(True)

        taipower = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
                    - current_p['sp_real_time']

        item01 = QtWidgets.QTableWidgetItem(pre_check(taipower))
        item01.setForeground(brush1)
        item01.setBackground(brush2)
        item01.setFont(font)
        item01.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(0, 1, item01)

        item11 = QtWidgets.QTableWidgetItem(pre_check(current_p['2H120':'5KB19'].sum()))
        item11.setForeground(brush1)
        item11.setBackground(brush3)
        item11.setFont(font)
        item11.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(1, 1, item11)

        item21 = QtWidgets.QTableWidgetItem(pre_check(current_p['sp_real_time'], b=5))
        item21.setForeground(brush1)
        item21.setBackground(brush4)
        item21.setFont(font)
        item21.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(2, 1, item21)

        item31 = QtWidgets.QTableWidgetItem(pre_check(current_p['feeder 1510':'feeder 1520'].sum()))
        item31.setForeground(brush1)
        item31.setBackground(brush5)
        item31.setFont(font)
        item31.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.tableWidget_3.setItem(3, 1, item31)

    def update_current_value(self):
        """
        用來每隔11秒，自動更新current value
        :return:
        """
        while True:
            self.dashboard_value()
            time.sleep(11)

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

        schedule = scrapy_schedule()
        if len(schedule) == 0:
            self.label_18.setStyleSheet("color:black")
            self.label_18.setText('目前無排程')
        else:
            self.label_18.setStyleSheet("color:red")
            self.label_18.setText('下一爐時間： ' + str(schedule[0][0].time()))

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
        """
        mbox.setText(content)   # 通知文字
        mbox.addButton(QtWidgets.QMessageBox.StandardButton.Ok)
        mbox.setIcon(QtWidgets.QMessageBox.Icon.Question)  # 加入問號 icon
        mbox.exec()
        """


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    myWin = MyMainForm()
    myWin.show()
    sys.exit(app.exec())
from logging_utils import setup_logging, log_exceptions, timeit, get_logger

setup_logging("logs/app.log", level="INFO")
logger = get_logger(__name__)

import sys, re, math
from typing import Tuple, Optional
import pandas as pd
from PyQt6 import QtCore, QtWidgets, QtGui
from PyQt6.QtGui import QLinearGradient
from UI import Ui_MainWindow
from tariff_version import get_current_rate_type_v6, get_ng_generation_cost_v2, format_range
from make_item import make_item
from visualization import TrendChartCanvas, TrendWindow, plot_tag_trends # 引入數據可視化模組
from ui_handler import setup_ui_behavior
from data_sources.pi_client import PIClient
from data_sources.schedule_scraper import scrape_schedule
from data_sources.data_analysis import (estimate_speed_from_last_peaks, analyze_production_avg_cycle,
                                        analyze_production_single_cycle)


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

class MyMainForm(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super(MyMainForm, self).__init__()
        self.setupUi(self)

        self.tag_list = pd.read_excel('.\parameter.xlsx', sheet_name=0).dropna(how='all')
        self.special_dates = pd.read_excel('.\parameter.xlsx', sheet_name=1)
        self.unit_prices = pd.read_excel('.\parameter.xlsx', sheet_name=2, index_col=0)
        self.time_of_use = pd.read_excel('.\parameter.xlsx', sheet_name=3)

        # ---------------統一設定即時值、平均值的背景及文字顏色----------------------
        self.real_time_text = "#145A32"   # 即時量文字顏色 深綠色文字
        self.real_time_back = "#D5F5E3"   # 即時量背景顏色 淡綠色背景
        self.average_text = "#154360"     # 平均值文字顏色 深藍色文字
        self.average_back = "#D6EAF8"     # 平均值背景顏色 淡藍色背景
        self.history_datas_of_groups = pd.DataFrame()  # 用來紀錄整天的各負載分類的週期平均值
        self.dashboard_value()
        # 建立趨勢圖元件並加入版面配置
        self.trend_chart = TrendChartCanvas(self)
        setup_ui_behavior(self)

        # --- 等待放到 ui_handler.py ---
        self.pushButton_6.clicked.connect(self.analyze_hsm)
        self.pushButton_9.clicked.connect(self.on_show_trend)
        self.listWidget_2.addItems(['HSM 軋延機組'])
        self.listWidget_2.addItems([str(name) for name in self.tag_list['name']])
        self.listWidget_2.itemDoubleClicked.connect(self.add_target_tag_to_list3)
        self.listWidget_3.itemDoubleClicked.connect(self.remove_target_tag_from_list3)
        self.radioButton_5.setChecked(True)

        # 取得目前的日期與時間，並捨去分鐘與秒數，將時間調整為整點
        current_datetime = QtCore.QDateTime.currentDateTime()
        rounded_current_datetime = current_datetime.addSecs(
            -current_datetime.time().minute() * 60 - current_datetime.time().second())

        # 設定結束時間為目前整點時間
        self.dateTimeEdit_4.setDateTime(rounded_current_datetime)

        # 設定起始時間為結束時間的前兩小時
        start_datetime = rounded_current_datetime.addSecs(-7200)  # 前兩小時
        self.dateTimeEdit_3.setDateTime(start_datetime)
        self.dateTimeEdit_5.setDateTime(rounded_current_datetime.addSecs(-900))

    def remove_target_tag_from_list3(self, item: QtWidgets.QListWidgetItem):
        row = self.listWidget_3.row(item)           # 取得該 item 所在的列號
        taken = self.listWidget_3.takeItem(row)     # 從listWidget_3 拿出(並移除) 該item
        del taken                                   # del 掉這些物件，避免記憶體累積和洩漏

    def add_target_tag_to_list3(self, item: QtWidgets.QListWidgetItem):
        name = item.text()
        self.listWidget_3.addItems([name])
        #self.listWidget_3.addItems(self.tag_list.loc[self.tag_list['name'] == name, 'tag_name2'])

    def analyze_hsm(self):
        """ 試調分析 HSM 用電資訊 """
        # -- 設定區 --
        interval = 2
        tag_reference = self.tag_list.set_index('name').copy()
        tags = tag_reference.loc['9H140':'9KB33', 'tag_name'].tolist()
        start = pd.Timestamp(self.dateTimeEdit_5.dateTime().toString())
        end = pd.Timestamp(self.dateTimeEdit_5.dateTime().toString()) + pd.offsets.Minute(self.spinBox_5.value())

        # 從PI 系統抓資料
        df = pi_client.query(start, end, tags, 'AVERAGE', f'{interval}s', 'ffill')

        # 將資料分類
        # 取出 9h140~9h280、9h180~9kb33 的欄位名稱list
        cols = (list(df.loc[:,'W511_HSM/33KV/9H_140/P':'W511_HSM/33KV/9H_280/P'].columns) +
                list(df.loc[:,'W511_HSM/33KV/9H_180/P':'W511_HSM/11.5KV/9KB1_2_33/P'].columns))

        original_date = pd.DataFrame(df[cols].sum(axis=1),columns=['Main_group'])
        filter_date = df.loc[:,'W511_HSM/33KV/9H_160/P':'W511_HSM/33KV/9H_170/P'].sum(axis=1)

        # 呼叫 data_analysis 的 analyze_production_avg_cycle
        res3 = analyze_production_avg_cycle(original_date['Main_group'], threshold=self.spinBox_3.value(),
                                            smooth_window=int(40/interval), prominence=self.spinBox_4.value(),
                                            power_filter=filter_date, plot=True)

    def on_show_trend(self):
        """趨勢圖測試區"""
        interval = 2
        tags = []
        tags2 = []
        tag_reference = self.tag_list.set_index('name').copy()

        # 1. 先決定 tag 與區間，可由 UI 元件收集
        for i in range(self.listWidget_3.count()):
            if self.listWidget_3.item(i).text() == 'HSM 軋延機組':
                if self.radioButton_5.isChecked():
                    tags.extend(tag_reference.loc['9H140':'9KB33','tag_name2'].tolist())
                else:
                    tags.extend(tag_reference.loc['9H140':'9KB33', 'tag_name'].tolist())
            else:
                if self.radioButton_5.isChecked():
                    tags.extend(tag_reference.loc[
                                    tag_reference.index == self.listWidget_3.item(i).text(), 'tag_name2'].tolist())
                else:
                    tags.extend(tag_reference.loc[
                                    tag_reference.index == self.listWidget_3.item(i).text(), 'tag_name'].tolist())
        tags2.extend(tag_reference.loc['9H160':'9H170', 'tag_name'].tolist())
        start = pd.Timestamp(self.dateTimeEdit_3.dateTime().toString())
        end = pd.Timestamp(self.dateTimeEdit_4.dateTime().toString())

        # 2. 抓資料
        if not tags:
            self.statusBar().showMessage("尚未選擇要顯示的迴路！！")
            return

        if self.radioButton_5.isChecked():     # --- 用 kwh 反推 ---
            df = pi_client.query(start, end, tags, 'RANGE', f'{interval}s', 'ffill')
            df = df * 3600 / interval
        else:                   # --- 用 p值讀資料 ---
            df = pi_client.query(start, end, tags, 'AVERAGE', f'{interval}s', 'ffill')

        if self.checkBox_3.isChecked():
            df = pd.DataFrame(df.sum(axis=1),columns=['add'])
            tags.append('add')

        # 3. 畫圖
        fig, _ = plot_tag_trends(df, tags, title="用電趨勢圖")

        # 4. 開新窗
        self._trend_win = TrendWindow(fig, self)  # 持有引用避免被 GC
        self._trend_win.show()

        """
        # 5. FFT 分析
        if self.checkBox_4.isChecked():
            #main_period, freqs, amps = detect_periodicity(df['add'], 1/interval)

            res2 = estimate_speed_from_last_peaks(df['add'], height=self.spinBox_3.value(), smooth_window=20,
                                                  prominence=self.spinBox_4.value())
            print("------即時用的生產速度-----------")
            if not res2['rest']:
                print("(1) 最後兩峰間隔:", res2['dt_s'], " 秒")
                print("(2) 生產速度:", res2['rate_items_per_15min'], " 卷/15分鐘")
                #print("(3) 這段週期內秏電:", res2['energy_interval_kwh'], " kwh")
                print("(4) 每卷需量:", res2['demand_per_item'], " MW \n")
            else:
                print(res2['error'])

            res = analyze_production_single_cycle(df['add'], threshold=self.spinBox_3.value(),
                                                     smooth_window=20, prominence=self.spinBox_4.value(),
                                                     power_filter=df2['filter'], plot=True)
            print("-----power_filter + smoothed + 以第一/最後完整週期長度推估 unfinished------------")
            print("(1) 生產速度：", res['rate_items_per_15min'], "卷/15 分鐘")
            print("(2) 總共生產件數：", res['total_items'], "卷")
            print("(3) 15分鐘需量:", res['demand_15m'], " MW \n")

            res3 = analyze_production_avg_cycle(df['add'], threshold=self.spinBox_3.value(),
                                                     smooth_window=20, prominence=self.spinBox_4.value(),
                                                     power_filter=df2['filter'], plot=True)
            print("-----power_filter + smoothed + 以所有完整週期平均長度推估 unfinished：------------")
            print("(1) 生產速度：", res3['rate_items_per_15min'], "卷/15 分鐘")
            print("(2) 總共生產件數：", res3['total_items'], "卷")
            print("(3) 15分鐘需量:", res3['demand_15m'], " MW \n")
        """

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """
            ❶ 只要覆寫 closeEvent 就能攔截使用者關窗的動作
            用來強制關閉thread_1、thread_2
        """
        for th in (getattr(self, "thread_1", None), getattr(self, "thread_2", None)):
            if isinstance(th, QtCore.QThread):
                th.requestInterruption()  # ① 送出中斷旗標
                th.wait(3000)  # ② 最多等 3 秒；0＝無限等
                if th.isRunning():  # ③ 保險：實在關不掉就強制殺
                    th.terminate()
                    th.wait()
        # ★ 若還有額外資源（計時器、連線等）也可在這裡一併釋放
        super().closeEvent(event)   # 呼叫父類別，讓 Qt 正常處理關窗

    def initialize_cost_benefit_widgets(self):
        # 取得目前的日期與時間，並捨去分鐘與秒數，將時間調整為整點
        current_datetime = QtCore.QDateTime.currentDateTime()
        rounded_current_datetime = current_datetime.addSecs(
            -current_datetime.time().minute() * 60 - current_datetime.time().second())

        # 設定結束時間為目前整點時間
        self.dateTimeEdit_2.setDateTime(rounded_current_datetime)

        # 設定起始時間為結束時間的前兩小時
        start_datetime = rounded_current_datetime.addSecs(-7200)  # 前兩小時
        self.dateTimeEdit.setDateTime(start_datetime)

        # 起始和結束的日期/時間有變更時，執行時間長度的計算和更新顯示
        self.dateTimeEdit.dateTimeChanged.connect(self.update_duration_label)
        self.dateTimeEdit_2.dateTimeChanged.connect(self.update_duration_label)
        self.update_duration_label()

        # tableWidget_4 和 tableWidget_5 不顯示垂直表頭
        self.tableWidget_4.verticalHeader().setVisible(False)
        self.tableWidget_5.verticalHeader().setVisible(False)
        self.tableWidget_4.horizontalHeader().setVisible(False)
        self.tableWidget_5.horizontalHeader().setVisible(False)

        self.update_benefit_tables(initialize_only=True)

    def tws_init(self):
        """
        1. 初始化所有treeWidget, tableWidget
        2. 因為treeWidget 的item 文字對齊方式，不知道為何從ui.ui 轉成UI.py 時，預設值都跑掉，所以只能先暫時在這邊設置

        :return:
        """

        # 定義顏色
        brush_sub = QtGui.QBrush(QtGui.QColor(180, 180, 180))  # 用於第 2 層及以上的即時量數值
        brush_sub.setStyle(QtCore.Qt.BrushStyle.SolidPattern)

        brush_top = QtGui.QBrush(QtGui.QColor(self.real_time_text))  # 用於 tw1 的頂層數值
        brush_top.setStyle(QtCore.Qt.BrushStyle.SolidPattern)

        # 遍歷 tw1, tw2, tw3，並統一初始化子項目
        for tree in [self.tw1, self.tw2, self.tw3]:
            for i in range(tree.topLevelItemCount()):
                # tw1 需要額外設定頂層的文字顏色，tw2 和 tw3 則不需要
                self.init_tree_item(tree.topLevelItem(i), level=0,
                               level0_color=(brush_top if tree == self.tw1 else None),
                               level_sub_color=brush_sub)

    def init_tree_item(self, item, level, level0_color=None, level_sub_color=None):
        """
        遞迴初始化 TreeWidgetItem 的對齊方式與文字顏色。

        設定方式：
          - 頂層 (level == 0)：
              - 第 0 欄置中，第 1、2 欄置右
              - tw1 頂層的即時量 (第 1 欄) 設定為 self.real_time_text
          - 次層 (level == 1)：
              - 第 0 欄置左，第 1、2 欄置右
          - 更深層 (level ≥2)：
              - 第 0 欄置中，第 1、2 欄置右，且即時量 (第 1 欄) 設定為 灰色
        """

        # 設定欄位對齊方式
        align0 = QtCore.Qt.AlignmentFlag.AlignCenter if level != 1 else QtCore.Qt.AlignmentFlag.AlignLeft
        align1 = QtCore.Qt.AlignmentFlag.AlignRight
        align2 = QtCore.Qt.AlignmentFlag.AlignRight

        item.setTextAlignment(0, align0)
        item.setTextAlignment(1, align1)
        item.setTextAlignment(2, align2)

        # 設定顏色
        if level == 0 and level0_color is not None:
            item.setForeground(1, level0_color)  # 頂層即時量顏色 (僅 tw1)
        elif level >= 2 and level_sub_color is not None:
            item.setForeground(1, level_sub_color)  # 內層即時量顏色

        # 遞迴處理子節點
        for i in range(item.childCount()):
            self.init_tree_item(item.child(i), level + 1, level0_color, level_sub_color)

    def beautify_tree_widgets(self):
        """ 美化 tw1, tw2, tw3 的即時量與平均值欄位，並區分不同表頭顏色 """
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
        self.tw4.setColumnWidth(0, 190)  # **排程時間**
        self.tw4.setColumnWidth(1, 200)  # **狀態**

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
        item = self.tableWidget_3.item(0, 0)
        gradient = QLinearGradient(0,0,1,1)      # 設定比例
        gradient.setCoordinateMode(QLinearGradient.CoordinateMode.ObjectBoundingMode)     # 讓漸層根據 item 大小調整
        gradient.setColorAt(0, QtGui.QColor("#52e5e7"))
        gradient.setColorAt(1, QtGui.QColor("#130cb7"))
        brush = QtGui.QBrush(gradient)
        item.setBackground(brush)       # 設定漸層背景 (與tw1,2 header 相同的漸層配色)
        item.setForeground((QtGui.QBrush(QtGui.QColor('white'))))   # 設定文字顏色為白色

        # 設定總類加總 (中龍發電量) 的配色
        item = self.tableWidget_3.item(1, 0)
        gradient.setColorAt(0, QtGui.QColor("#0e6499"))
        gradient.setColorAt(1, QtGui.QColor("#9fdeab"))
        brush = QtGui.QBrush(gradient)
        item.setBackground(brush)       # 設定漸層背景 (與tw3 header 相同的漸層配色)
        item.setForeground((QtGui.QBrush(QtGui.QColor('white'))))   # 設定文字顏色為白色

        self.tableWidget_3.setItem(2, 0, make_item('太陽能', bold=False, bg_color='#f6ffc6',font_size=12))
        self.tableWidget_3.setItem(3, 0, make_item('台電供電量\n(需量)', bold=False, font_size=8.5))

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
        """
            比對歷史紀錄的勾選值變動時，DashBoard 頁面中的tree widget(1~3)、table widget 3
            其表格、欄位大小、顯示與否進行調整。
        """
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
                ### 切換負載的顯示方式 ###
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
            self.tw2.topLevelItem(4).setText(0,'1H450')
            self.tw2.topLevelItem(5).setText(0,'1H360')
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
        ### 處理 Dashboard 各表格的即時量呈現，製程排程的更新 ###
        1. 從 parameter.xlse 讀取出tag name 相關對照表, 轉換為list 指定給的 name_list這個變數
        2. 透過pi_client 類別實例中的方法，一次性搜尋多個tag 的PIPoint 物件，並透過PIPoint 的屬性，
           向 PI Data Archive 發出一次性查詢，並把結果用 pd.Series (tag_name, current_value)
           的型式回傳，其中current_value 已被強制從object->float，如有文字，則用Nan取代。
        3. 透過 pd.merge() 的方法，把tag_list 其色columns 以tag_name為中心做關聯式合併。
        4. 從 buffer 這個dataframe 取出 value 這一列，而index 則採用name 這一列。
        5. 利用 group by 的功能，依Group1(單位)、Group2(負載類型)進行分組，將分組結果套入sum()的方法
        6. 使用slice (切片器) 來指定 MultiIndex 的範圍，指定各一級單位B類型(廠區用電)的計算結果，
           指定到wx 這個Series,並重新設定index
        7. 將wx 內容新增到c_values 之後。
        :return:
        """

        name_list = self.tag_list['tag_name'].dropna().tolist()      # 1
        current = pi_client.current_values(name_list)       # 2
        buffer = pd.DataFrame({
            'tag_name': name_list,
            'value': current.values
        })
        buffer = pd.merge(self.tag_list, buffer, on='tag_name')  # 3
        c_values = buffer.loc[:,'value']
        c_values.index = buffer.loc[:,'name']     # 4
        wx_grouped = buffer.groupby(['Group1','Group2'])['value'].sum()     # 5
        wx = wx_grouped.loc[(slice('W2','WA')),'B']      # 6
        wx.index = wx.index.get_level_values(0)
        c_values = pd.concat([c_values, wx],axis=0)  # 7
        self.tws_update(c_values)
        self.label_23.setText(str(f'%s MW' %(self.predict_demand())))

    def update_tw4_schedule(self):
        """
        ### 更新 tw4 (treeWidget) 顯示 scrapy_schedule() 解析的排程資訊：###
        - 第一層：製程種類 (EAF, LF1-1, LF1-2)
        - 第二層："生產或等待中" (current + future) / "過去排程" (past)
        - 若無 "生產或等待中" 排程，仍增加此分類，但不增加子排程，並顯示 "目前無排程"
        - 若無 "過去排程" 資料，仍增加此分類，但不增加子排程，並顯示 "無相關排程"
        - **column 2 (狀態欄) 文字置中**
        """
        past_df, current_df, future_df, status = scrape_schedule()
        if status == "ERROR":
            # showMessage(text, timeout_ms)：timeout_ms 單位是毫秒，
            # 若 timeout_ms = 0，訊息就會一直停留，不會自動消失。
            self.statusBar().showMessage("⚠ 撈取排程資料失敗，請檢查網路或來源", 10000)
            # 以上讓訊息顯示 5 秒後自動消失。如果要一直顯示，第二個參數可改成 0。
            # 例如： self.statusBar().showMessage("⚠ 排程撈取失敗", 0)
        else:
            self.statusBar().clearMessage()

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
            active_parent.setFont(0, QtGui.QFont("微軟正黑體", 10))
            active_parent.setText(0, "生產或等待中")
            process_parent.addChild(active_parent)

            if not active_schedules.empty:
                """
                從iterrows() 改為itertuples() 的說明:
                1. 效能較快、且省記憶體
                2. itertuples(index=False)：避免產生多餘的 Index 欄位。
                2. row.開始時間、row.類別 等是透過屬性方式存取。
                3. hasattr(row, "製程狀態") 是為了避免製程狀態 欄位在某些 DataFrame 裡不存在（如 future_df），防止程式報錯。
                """
                for row in active_schedules.itertuples(index=False):
                    start_time = row.開始時間.strftime("%H:%M:%S")
                    end_time = row.結束時間.strftime("%H:%M:%S")
                    category = row.類別
                    status = str(row.製程狀態) if hasattr(row, "製程狀態") and pd.notna(row.製程狀態) else "N/A"

                    if row.製程 == "EAFA":
                        process_display = "EAF"
                        status += " (A爐)"
                        furnace = "(A爐)"
                    elif row.製程 == "EAFB":
                        process_display = "EAF"
                        status += " (B爐)"
                        furnace = "(B爐)"
                    else:
                        process_display = row.製程
                        furnace = ""
                    if process_display != process_name:
                        continue

                    item = QtWidgets.QTreeWidgetItem(active_parent)
                    item.setFont(0, QtGui.QFont("微軟正黑體", 10))
                    item.setFont(1, QtGui.QFont("微軟正黑體", 10))
                    item.setText(0, f"{start_time} ~ {end_time}")
                    item.setText(1, status)

                    # **狀態欄 (column 2) 文字置中**
                    item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

                    if category == "current":
                        item.setBackground(0, QtGui.QBrush(QtGui.QColor("#FCF8BC")))  # **淡黃色背景**
                        item.setBackground(1, QtGui.QBrush(QtGui.QColor("#FCF8BC")))
                    elif category == "future":
                        minutes = int((row.開始時間 - pd.Timestamp.now()).total_seconds() / 60)
                        if process_name == "EAF":
                            item.setText(1, f"{furnace} 預計{minutes} 分鐘後開始生產")
                        else:
                            item.setText(1, f"預計{minutes} 分鐘後開始生產")
                        item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)  # **未來排程置中**

                    active_parent.addChild(item)

            else:
                # **若無生產或等待中排程，在 column 2 顯示 "目前無排程"，並置中**
                active_parent.setFont(1, QtGui.QFont("微軟正黑體", 10))
                active_parent.setText(1, "目前無排程")
                active_parent.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

            # **處理 "過去排程"**
            past_parent = QtWidgets.QTreeWidgetItem(process_parent)
            past_parent.setFont(0, QtGui.QFont("微軟正黑體", 10))
            past_parent.setText(0, "過去排程")
            process_parent.addChild(past_parent)

            if not past_schedules.empty:
                for _, row in past_schedules.iterrows():
                    start_time = row["開始時間"].strftime("%H:%M:%S")
                    end_time = row["結束時間"].strftime("%H:%M:%S")

                    item = QtWidgets.QTreeWidgetItem(past_parent)
                    item.setFont(0, QtGui.QFont("微軟正黑體", 10))
                    item.setFont(1, QtGui.QFont("微軟正黑體", 10))
                    item.setText(0, f"{start_time} ~ {end_time}")
                    item.setText(1, "已完成")
                    item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)  # **過去排程置中**

                    past_parent.addChild(item)

            else:
                # **若無過去排程，在 column 2 顯示 "無相關排程"，並置中**
                past_parent.setFont(1, QtGui.QFont("微軟正黑體", 10))
                past_parent.setText(1, "無相關排程")
                past_parent.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignCenter)

        # **確保所有節點展開**
        self.tw4.expandAll()  # ✅ 確保所有製程展開

    def predict_demand(self):
        """
        ### 計算預測的demand。目前預測需量的計算方式為， ###
        目前週期的累計需量值 + 近180秒的平均需量 / 180 x 該剩期剩餘秒數
        :return:
        """
        st = pd.Timestamp.now().floor('15T')    # 目前週期的起始時間
        et = st + pd.offsets.Minute(15)         # 目前週期的結束時間

        back_300s_from_now = pd.Timestamp.now() - pd.offsets.Second(300)    # 300秒前的時間點 (180->300)
        diff_between_now_and_et = (et - pd.Timestamp.now()).total_seconds()   # 此週期剩餘時間

        tags = self.tag_list.loc[0:1,'tag_name2']
        tags.index = self.tag_list.loc[0:1,'name']
        name_list = tags.loc[:].dropna().tolist()

        # 查詢目前週期的累計需量值
        query_result = pi_client.query(st=st, et=et, tags=name_list)
        current_accumulation = query_result.sum(axis = 1) * 4


        # 查近180秒的平均需量，並計算出剩餘時間可能會增加的需量累計值
        result = pi_client.query(st=back_300s_from_now, et=back_300s_from_now + pd.offsets.Second(180),
                            tags=name_list)
        weight2 = 4 / 180 * diff_between_now_and_et
        predict = result.sum(axis=1) * weight2

        # 取四捨五入
        demand = round((current_accumulation[0] + predict[0]),2)
        return demand

    def history_demand_of_groups(self, st, et):
        """
            ### 查詢特定週期，各設備群組(分類)的平均值 ###
        :param st: 查詢的起始時間點
               et: 查詢的最終時間點
        :return:
        """
        mask = ~pd.isnull(self.tag_list.loc[:,'tag_name2'])     # 作為用來篩選出tag中含有有kwh11 的布林索引器
        groups_demand = self.tag_list.loc[mask, 'tag_name2':'Group2']
        groups_demand.index = self.tag_list.loc[mask,'name']
        name_list = groups_demand.loc[:,'tag_name2'].dropna().tolist() # 把DataFrame 中標籤名為tag_name2 的值，轉成list輸出
        query_result = pi_client.query(st=st, et=et, tags=name_list)
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
        now = pd.Timestamp.now()
        current_date = pd.Timestamp(self.dateEdit_3.date().toString())
        # 依據水平捲軸的值計算所選的區間
        st = current_date + pd.offsets.Minute(15) * self.horizontalScrollBar.value()
        et = st + pd.offsets.Minute(15)

        # 如果查詢日期為今天，檢查是否需要刷新歷史資料
        if current_date.normalize() == now.normalize():
            # 過濾出符合時間格式的欄位，取得目前已查詢的最晚時間欄位

            time_columns = [col for col in self.history_datas_of_groups.columns if re.match(r'^\d{2}:\d{2}$', str(col))]
            # 過濾掉全部為 NaN 的欄位
            valid_time_columns = [t for t in time_columns if self.history_datas_of_groups[t].dropna().size > 5]
            if valid_time_columns:
                last_completed_time_str = max(valid_time_columns,
                                              key=lambda t: pd.Timestamp(f"{current_date.date()} {t}"))
                max_time = pd.Timestamp(f"{current_date.date()} {last_completed_time_str}")

            # 如果目前系統時間已超過這個時間（表示有新完成的區間）
            #if now > max_time:
            if et > max_time:
                # 重新查詢整天的歷史資料更新到最新狀態
                self.history_demand_of_groups(st=current_date, et=current_date + pd.offsets.Day(1))

        # 如果選取的區間 et 超過目前時間，則調整至最後完成的區間
        if et > now:
            et = now.floor('15T')
            # 重新計算對應的水平捲軸值
            self.horizontalScrollBar.setValue(((et - current_date) // pd.Timedelta('15T')) - 1)
            st = et - pd.offsets.Minute(15)

        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))
        # 更新畫面顯示歷史資料（以 st 的時間作為 column key）
        self.update_history_to_tws(self.history_datas_of_groups.loc[:, st.strftime('%H:%M')])

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

        w41_utility = current_p['W4']
        w42_utility = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_utility = w41_utility + w42_utility
        w41_main = current_p['AJ130':'AJ170'].sum()
        w4_total = w41_main + w4_utility

        self.tw1.topLevelItem(2).setText(2, pre_check2(w4_total, b=0))
        self.tw1.topLevelItem(2).child(0).setText(2, pre_check2(w41_main,b=0))
        self.tw1.topLevelItem(2).child(1).setText(2, pre_check2(w4_utility,b=0))

        w5_subtotal = current_p['3KA14':'2KB29'].sum() + current_p['W5']
        # self.tw1.topLevelItem(3).setText(2,pre_check2(w5_total))
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

        self.tw2.topLevelItem(0).setText(2, pre_check2(current_p['9H140':'9KB33'].sum(),b=0))
        self.tw2.topLevelItem(1).setText(2, pre_check2(current_p['AH120'],b=0))
        self.tw2.topLevelItem(2).setText(2, pre_check2(current_p['AH190'],b=0))
        self.tw2.topLevelItem(3).setText(2, pre_check2(current_p['AH130'],b=0))
        self.tw2.topLevelItem(4).setText(2, pre_check2(current_p['1H450'],b=0))
        self.tw2.topLevelItem(5).setText(2, pre_check2(current_p['1H360'],b=0))

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
        tai_power_demand = current_p['feeder 1510':'feeder 1520'].sum()
        reversed_power = current_p['feeder 1510_s':'feeder 1520_s'].sum()
        full_load = tai_power_demand - reversed_power + current_p['2H120':'5KB19'].sum() - sun_power


        self.update_table_item(0, 2, pre_check2(full_load), self.average_back, self.average_text, bold=True)
        self.update_table_item(1, 2, pre_check2(current_p['2H120':'5KB19'].sum()), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(2, 2, pre_check2(sun_power, b=5), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(3, 2, str(format(round(tai_power_demand,2))), self.average_back,
                               self.average_text, bold=True)

        # error_value & w5_total correction
        dynamic_load = current_p['AH120':'9KB33'].sum()
        error_value = (full_load -w2_total - w3_total -w4_total - w5_subtotal - dynamic_load - current_p['WA'])
        self.tw1.topLevelItem(3).child(6).setText(2, str(format(round(error_value, 2), '.2f')))
        w5_total = w5_subtotal + error_value
        self.tw1.topLevelItem(3).setText(2, pre_check2(w5_total))

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

        w41_utility = current_p['W4']
        w42_utility = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_utility = w41_utility + w42_utility
        w41_main = current_p['AJ130':'AJ170'].sum()
        w4_total = w41_main + w4_utility

        self.tw1.topLevelItem(2).setText(1, pre_check(w4_total))
        self.tw1.topLevelItem(2).child(0).setText(1, pre_check(w41_main, b=4))
        self.tw1.topLevelItem(2).child(1).setText(1, pre_check(w4_utility))

        w5_subtotal = current_p['3KA14':'2KB29'].sum() + current_p['W5']
        self.tw1.topLevelItem(3).setText(1,pre_check(w5_subtotal))
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
        self.tw2.topLevelItem(4).setText(1, pre_check(current_p['1H450'], 0))
        self.tw2.topLevelItem(5).setText(1, pre_check(current_p['1H360'], 0))

        ng_to_power = get_ng_generation_cost_v2(self.unit_prices).get("convertible_power")
        #ng_to_power = self.unit_prices.loc['可轉換電力', 'current']

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
        full_load = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
                    - current_p['sp_real_time']
        tai_power_demand = str(format(round(current_p['feeder 1510':'feeder 1520'].sum(), 2), '.2f')) + ' MW'

        self.update_table_item(0, 1, pre_check(full_load), self.real_time_back, self.real_time_text)
        self.update_table_item(1, 1, pre_check(current_p['2H120':'5KB19'].sum()), self.real_time_back, self.real_time_text)  # 即時量
        self.update_table_item(2, 1, pre_check(current_p['sp_real_time'], b=5), self.real_time_back, self.real_time_text)
        self.update_table_item(3, 1, tai_power_demand , self.real_time_back, self.real_time_text)

        # error_value & w5_total correction
        dynamic_load = current_p['AH120':'9KB33'].sum()
        error_value = (full_load -w2_total - w3_total -w4_total - w5_subtotal - dynamic_load - current_p['WA'])
        self.tw1.topLevelItem(3).child(6).setText(1, str(format(round(error_value, 2), '.2f'))+ ' MW')
        w5_total = w5_subtotal + error_value
        self.tw1.topLevelItem(3).setText(1, pre_check(w5_total))

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
        while not QtCore.QThread.currentThread().isInterruptionRequested():
            self.dashboard_value()
            QtCore.QThread.msleep(11_000)  # 用 Qt 的 sleep，不卡 event-loop

    def continuously_scrapy_and_update(self):
        while not QtCore.QThread.currentThread().isInterruptionRequested():
            self.update_tw4_schedule()
            QtCore.QThread.msleep(30_000)


    def tw3_expanded_event(self):
        """
        處理 tw3 展開與收縮事件：
          - 當某個 top-level 項目展開時，將其第一欄文字對齊方式改為左對齊，
            並將其第二欄文字前景色設為透明（隱藏文字）。
          - 當收縮時，第一欄置中，第二欄恢復為黑色。
        """
        b_transparent = QtGui.QBrush(QtGui.QColor(0, 0, 0, 0))
        b_solid = QtGui.QBrush(QtGui.QColor(0, 0, 0, 255))

        # 遍歷 tw3 的所有 top-level 項目 (例如：TGs, TRTs, CDQs)
        for i in range(self.tw3.topLevelItemCount()):
            item = self.tw3.topLevelItem(i)
            if item.isExpanded():
                item.setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
                item.setForeground(1, b_transparent)
            else:
                item.setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
                item.setForeground(1, b_solid)

    def tw1_expanded_event(self):
        """
        處理 tw1 展開與收縮事件，根據各層項目是否展開，設定文字對齊方式及前景色：
          - 當 top-level 項目展開時，第一欄與第二欄皆置左，
            否則第一欄置中，第二欄置右。
          - 對於特定子項目，若展開則將其文字設為透明，不展開則恢復為不透明（黑色）。
        """
        b_transparent = QtGui.QBrush(QtGui.QColor(0, 0, 0, 0))
        b_solid = QtGui.QBrush(QtGui.QColor(0, 0, 0, 255))

        def update_alignment(item):
            if item.isExpanded():
                item.setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignLeft)
                item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignLeft)
            else:
                item.setTextAlignment(0, QtCore.Qt.AlignmentFlag.AlignCenter)
                item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)

        def update_child_foreground(parent, child_index):
            child = parent.child(child_index)
            if child.isExpanded():
                child.setForeground(1, b_transparent)
            else:
                child.setForeground(1, b_solid)

        # 建立 top-level 項目與其需更新的子項目索引對應關係
        update_children = {
            0: [0, 2, 3],  # w2: 依序更新「鼓風機群」、「#2 燒結風車群」與「#2 屋頂風扇&runner 群」
            1: [1],  # w3: 更新「轉爐除塵」
            # 項目 2 (w4) 僅更新對齊，不需處理子項
            3: [0, 1, 2, 3, 4]  # w5: 分別更新 O2#1、O2#2、O2#3、空壓機群 與 IDF 群
        }

        # 遍歷所有 top-level 項目，更新對齊方式及子項前景色
        for i in range(self.tw1.topLevelItemCount()):
            top_item = self.tw1.topLevelItem(i)
            update_alignment(top_item)
            if i in update_children:
                for child_idx in update_children[i]:
                    if top_item.childCount() > child_idx:
                        update_child_foreground(top_item, child_idx)

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
        self.label_6.setStyleSheet("color:green; font-size:12pt;")
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
        raw_data = pi_client.query(st=st, et=et, tags=tags)
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
        buffer2 = pi_client.query(st=pd.Timestamp(cbl_date[-1]),
                           et=pd.Timestamp(cbl_date[0] + pd.offsets.Day(1)), tags=tags)
        row_data = (buffer2.iloc[:, 0] + buffer2.iloc[:, 1]) * 4  # 3
        """
            1. 每天要取樣的起始時間點, 存成list
            2. 將指定時間長度的需量，一天為一筆(pd.Series 的型態) 儲存至list
            3. 將list 中每筆Series name 更改為日期
        """
        period_start = [(cbl_date[i] + pd.Timedelta(str(self.timeEdit.time().toPyTime())))
                        for i in range(self.spinBox.value())]       # 1

        demands_buffer = list()
        for i in range(self.spinBox.value()):
            s_point = str(period_start[i])
            e_point = str(period_start[i] + pd.offsets.Minute((self.spinBox_2.value() * 4 - 1) * 15))
            demands_buffer.append(row_data.loc[s_point: e_point])                   # 2
            demands_buffer[i].rename(cbl_date[i].date(), inplace=True, copy=False)  # 3
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

    def remove_item_from_cbl_list(self):
        selected = self.listWidget.currentRow() # 取得目前被點撃item 的index
        self.listWidget.takeItem(selected) # 將指定index 的item 刪除

    def add_item_to_cbl_list(self):
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

    def update_duration_label(self):
        start_dt = self.dateTimeEdit.dateTime().toPyDateTime()
        end_dt = self.dateTimeEdit_2.dateTime().toPyDateTime()

        diff_secs = (end_dt - start_dt).total_seconds()
        if diff_secs < 0:
            self.label_26.setText("時間錯誤")
            return

        hours, remainder = divmod(diff_secs, 3600)
        minutes = remainder // 60
        self.label_26.setText(f"{int(hours):02d}時{int(minutes):02d}分")

    @log_exceptions()
    @timeit(level=20)
    def benefit_appraisal(self, *_):

        self.statusBar().showMessage("⏳🏃‍計算效益中，請稍後...🏃⏳", 100000)
        # 會短暫回到事件循環(只執行一次)，讓 statusBar().showMessage 先跑一次。
        QtWidgets.QApplication.processEvents()

        # **限制時間長度小於一定時間，而且不可以是負數的時間**
        if "錯誤" in self.label_26.text():
            self.show_box('起始時間必須比結束時間早！')
            return
        label = self.label_26.text().replace("時", ":").replace("分", "")
        try:
            h, m = map(int, label.split(":"))
            if h > 36:
                self.show_box('查詢時間不可大於36小時！')
                return
        except:
            return

        # ** 時間上的解析度設定 **
        t_resolution = 10
        t_resolution_str = f'{t_resolution}s'
        coefficient = t_resolution * 1000 / 3600 # 1000: MWH->KWH  3600: hour->second
        special_date = self.special_dates['台電離峰日'].tolist()

        st = pd.Timestamp(self.dateTimeEdit.dateTime().toString())
        et = pd.Timestamp(self.dateTimeEdit_2.dateTime().toString())
        if et > pd.Timestamp.now(): # ** 如果超過目前的時間，則取下取整到指定的單位)
            et = pd.Timestamp.now().floor(t_resolution_str)

        # ** 從PI 系統讀取的TAG 範圍 **
        target_names = ['feeder 1510','feeder 1520', '2H120', '2H220', '5H120', '5H220',
                        '1H120', '1H220', '1H320', '1H420', '4H120', '4H220', '4KA18',
                        '5KB19', 'TG1 NG', 'TG2 NG', 'TG3 NG', 'TG4 NG',]
        filter_list = self.tag_list[self.tag_list['name'].isin(target_names)]['tag_name']

        # ** 執行查詢PI 系統的函式，並將結果的columns 套上相對應的名稱
        raw_result = pi_client.query(st=st,et=et,tags=filter_list,summary="AVERAGE",interval=t_resolution_str)
        raw_result.columns = target_names

        # ** 開始計算相關效益 **
        cost_benefit = pd.DataFrame(raw_result.loc[:, 'feeder 1510':'feeder 1520'].sum(axis=1), columns=['即時TPC'])
        cost_benefit['中龍發電量'] = raw_result.loc[:, '2H120':'5KB19'].sum(axis=1)
        cost_benefit['全廠用電量'] = cost_benefit['即時TPC'] + cost_benefit['中龍發電量']
        cost_benefit['NG 總用量'] = raw_result.loc[:, 'TG1 NG':'TG4 NG'].sum(axis=1)

        # ** 用來記錄查詢區間，有用到那些版本的參數 **
        self.version_used = {} # 清空舊資料
        self.purchase_versions_by_period = {}
        self.sale_versions_by_period = {}
        self.version_info ={}
        ng_cost_versions = []
        ng_cost_keys = set()

        for ind in cost_benefit.index:
            # ** 根據 index 的時間，讀取適用各種日期版本的的單價 **
            """
            if par1:
                # ** 如果與該筆的日期符合上一筆的版本日期範圍，則不需再調用函式重新查表 **
                #ng_ver = (par1.get('ng_ver_start') <= ind) and ((ind < par1.get('ng_ver_end') if all(par1.get('ng_ver_end')) else True))
                print(par1.get('ng_price_ver_start'))
                if par1.get('ng_price_ver_start') <= ind:
                    if ind < par1.get('ng_price_ver_end'):
                        ng_ver = True
                heat_ver = (par1.get('heat_ver_start') <= ind) and (True if ind < par1.get('heat_ver_start') else False)
                if not(ng_ver and heat_ver):
                    par1 = get_ng_generation_cost_v2(self.unit_prices, ind)
            else:
                par1 = get_ng_generation_cost_v2(self.unit_prices, ind)

            if par2:
                purchase_ver = (par2.get('purchase_ver_start') <= ind) and (ind < par2.get('purchase_ver_start'))
                sale_ver = (par2.get('sale_ver_start') <= ind) and (ind < par2.get('sale_ver_start'))
                if not(purchase_ver and sale_ver):
                    par2 = get_ng_generation_cost_v2(self.unit_prices, ind)
            else:
                par2 = get_current_rate_type_v6(self.time_of_use, special_date, self.unit_prices, ind)
            """
            par1 = get_ng_generation_cost_v2(self.unit_prices, ind)
            par2 = get_current_rate_type_v6(self.time_of_use, special_date, self.unit_prices, ind)

            # 🔹 交集版本期間：開始為最大值，結束為最小值
            cost_start = max(
                par1.get("ng_price_ver_start"),
                par1.get("heat_ver_start")
            )
            cost_end = min(
                par1.get("ng_price_ver_end"),
                par1.get("heat_ver_end")
            ) if all([par1.get("ng_price_ver_end"), par1.get("heat_ver_end")]) else None

            range_text = format_range(cost_start, cost_end)

            key = (par1.get("ng_cost"), par1.get("tg_maintain_cost"), range_text)
            if key not in ng_cost_keys:
                ng_cost_keys.add(key)
                ng_cost_versions.append({
                    "value": par1.get("ng_cost"),
                    "tg_cost": par1.get("tg_maintain_cost"),
                    "start": cost_start.strftime("%Y/%m/%d") if cost_start else "",
                    "end": cost_end.strftime("%Y/%m/%d") if cost_end else "（目前）"
                })
            self.version_used["ng_cost_versions"] = ng_cost_versions

            period = par2.get("rate_label", "")
            if period:
                # 儲存「每個時段」的購電與售電單價版本
                if period not in self.purchase_versions_by_period:
                    self.purchase_versions_by_period[
                        period] = f"${par2['unit_price']:.2f}（{par2['purchase_range_text']}）"
                if period not in self.sale_versions_by_period:
                    self.sale_versions_by_period[period] = f"${par2['sale_price']:.2f}（{par2['sale_range_text']}）"

            # 🔹 NG 成本版本區間（交集）
            ng_cost_range = par1.get("ng_cost_range_text", "")
            if ng_cost_range:
                self.version_used["NG 成本"] = f"{ng_cost_range}（{par1.get('ng_cost', 0):.4f} 元/kWh）"

            # 🔹 其它 NG 參數
            if par1.get("ng_price_range_text"):
                self.version_used["NG 牌價"] = f"{par1['ng_price_range_text']}（{par1.get('ng_price', 0):.2f} 元/NM³）"
            if par1.get("heat_range_text"):
                self.version_used["熱值"] = f"{par1['heat_range_text']}（{par1.get('ng_heat', 0):.2f} kcal/NM³）"
            if par1.get("tg_range_text"):
                self.version_used[
                    "TG 維運成本"] = f"{par1['tg_range_text']}（{par1.get('tg_maintain_cost', 0):.4f} 元/kWh）"
            if par1.get("car_range_text"):
                self.version_used["碳費"] = f"{par1['car_range_text']}（{par1.get('carbon_cost', 0):.4f} 元/kWh）"
            if par1.get("steam_power"):
                f"{par1['car_range_text']}（{par1.get('carbon_cost', 0):.4f} 元/kWh）"

            # ** 用來提供tableWidget_5、6 欄位的tool_tip 訊息
            self.version_info[ind] = {
                "unit_price": {
                    "value": par2.get("unit_price"),
                    "version": par2.get("purchase_range_text")
                },
                "sale_price": {
                    "value": par2.get("sale_price"),
                    "version": par2.get("sale_range_text")
                }
            }

            cost_benefit.loc[ind, 'NG 購入成本'] = cost_benefit.loc[ind, 'NG 總用量'] * par1.get('ng_price') / 3600 * t_resolution
            cost_benefit.loc[ind, 'NG 增加的發電度數'] = (cost_benefit.loc[ind, 'NG 總用量'] * par1.get('convertible_power')
                                            / 3600 * t_resolution)
            cost_benefit.loc[ind, 'NG 增加的發電量'] = cost_benefit.loc[ind, 'NG 增加的發電度數'] / 1000 * 3600 / t_resolution
            cost_benefit.loc[ind, 'TG 增加的維運成本'] = cost_benefit.loc[ind, 'NG 增加的發電度數'] * par1.get('tg_maintain_cost')
            cost_benefit.loc[ind, '增加的碳費'] = cost_benefit.loc[ind, 'NG 增加的發電度數'] * par1.get('carbon_cost')
            cost_benefit.loc[ind, '原始TPC'] = cost_benefit.loc[ind, '即時TPC'] + cost_benefit.loc[ind, 'NG 增加的發電量']
            cost_benefit.loc[ind, '時段'] = par2.get('rate_label')
            # ** 根據原始TPC 是否處於逆送電，計算各種效益 **
            if cost_benefit.loc[ind, 'NG 總用量'] != 0:
                # ** 還原後TPC 處於逆送電時 **
                if cost_benefit.loc[ind, '原始TPC'] <= 0:
                    """ 
                        增加的售電收入 = NG 增加的發電量 * 躉售電售
                        增加售電的NG購入成本 = NG 增加的發電量 * NG發電成本
                        增加售電的TG 維運成本 = NG 增加的發電量 * TG 維運成本
                        增加售電的碳費 = NG 增加的發電量 * 碳費                        
                        降低的購電費用 = 0
                        降低購電的NG購入成本 = 0
                        降低購電的TG 維運成本 = 0
                        降低購電的碳費 = 0     
                    """
                    cost_benefit.loc[ind, '增加的售電收入'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par2.get('sale_price') * coefficient
                    cost_benefit.loc[ind, '增加售電的NG購入成本'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('ng_cost') * coefficient
                    cost_benefit.loc[ind, '增加售電的TG維運成本'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('tg_maintain_cost') * coefficient
                    cost_benefit.loc[ind, '增加售電的碳費'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('carbon_cost') * coefficient
                    cost_benefit.loc[ind, '降低的購電費用'] = 0
                    cost_benefit.loc[ind, '降低購電的NG購入成本'] = 0
                    cost_benefit.loc[ind, '降低購電的TG維運成本'] = 0
                    cost_benefit.loc[ind, '降低購電的碳費'] = 0
                # ** 還原後TPC 處於購電時 **
                else:
                    # ** NG 發電量 > 還原後的TPC **
                    if cost_benefit.loc[ind, 'NG 增加的發電量'] > cost_benefit.loc[ind, '原始TPC']:
                        """ 
                            增加的售電收入 = (NG 增加的發電量- 原TPC) * 躉售電售
                            增加售電的NG購入成本 = (NG 增加的發電量- 原TPC) * NG發電成本
                            增加售電的TG 維運成本 = (NG 增加的發電量- 原TPC) * TG 維運成本
                            增加售電的碳費 = (NG 增加的發電量- 原TPC) * 碳費                            
                            降低的購電費用 = 原TPC * 時段購電價
                            降低購電的NG購入成本 = 原TPC * NG發電成本
                            降低購電的TG 維運成本 = 原TPC * TG 維運成本
                            降低購電的碳費 = 原TPC * 碳費
                        """
                        cost_benefit.loc[ind, '增加的售電收入'] = (cost_benefit.loc[ind, 'NG 增加的發電量'] - cost_benefit.loc[ind, '原始TPC']) * par2.get('sale_price') * coefficient
                        cost_benefit.loc[ind, '增加售電的NG購入成本'] = (cost_benefit.loc[ind, 'NG 增加的發電量'] - cost_benefit.loc[ind, '原始TPC']) * par1.get('ng_cost') * coefficient
                        cost_benefit.loc[ind, '增加售電的TG維運成本'] = (cost_benefit.loc[ind, 'NG 增加的發電量'] - cost_benefit.loc[ind, '原始TPC']) * par1.get('tg_maintain_cost') * coefficient
                        cost_benefit.loc[ind, '增加售電的碳費'] = (cost_benefit.loc[ind, 'NG 增加的發電量'] - cost_benefit.loc[ind, '原始TPC']) * par1.get('carbon_cost') * coefficient

                        cost_benefit.loc[ind, '降低的購電費用'] = cost_benefit.loc[ind, '原始TPC'] * par2.get('unit_price') * coefficient
                        cost_benefit.loc[ind, '降低購電的NG購入成本'] = cost_benefit.loc[ind, '原始TPC'] * par1.get('ng_cost') * coefficient
                        cost_benefit.loc[ind, '降低購電的TG維運成本'] = cost_benefit.loc[ind, '原始TPC'] * par1.get('tg_maintain_cost') * coefficient
                        cost_benefit.loc[ind, '降低購電的碳費'] = cost_benefit.loc[ind, '原始TPC'] * par1.get('carbon_cost') * coefficient

                    # ** NG 發電量 <= 還原後的TPC
                    else:
                        """ 
                            增加的售電收入 = 0
                            增加售電的NG購入成本 = 0
                            增加售電的TG 維運成本 = 0
                            增加售電的碳費 = 0
                            降低的購電費用 = NG 增加的發電量 * 時段購電價
                            降低購電的NG購入成本 = NG 增加的發電量 * NG發電成本
                            降低購電的TG 維運成本 = NG 增加的發電量 * TG 維運成本
                            降低購電的碳費 = NG 增加的發電量 * 碳費
                        """
                        cost_benefit.loc[ind, '增加的售電收入'] = 0
                        cost_benefit.loc[ind, '增加售電的NG購入成本'] = 0
                        cost_benefit.loc[ind, '增加售電的TG維運成本'] = 0
                        cost_benefit.loc[ind, '增加售電的碳費'] = 0
                        cost_benefit.loc[ind, '降低的購電費用'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par2.get('unit_price') * coefficient
                        cost_benefit.loc[ind, '降低購電的NG購入成本'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('ng_cost') * coefficient
                        cost_benefit.loc[ind, '降低購電的TG維運成本'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('tg_maintain_cost') * coefficient
                        cost_benefit.loc[ind, '降低購電的碳費'] = cost_benefit.loc[ind, 'NG 增加的發電量'] * par1.get('carbon_cost') * coefficient

            else:
                cost_benefit.loc[ind, '增加的售電收入'] = 0
                cost_benefit.loc[ind, '增加售電的NG購入成本'] = 0
                cost_benefit.loc[ind, '增加售電的TG維運成本'] = 0
                cost_benefit.loc[ind, '增加售電的碳費'] = 0
                cost_benefit.loc[ind, '降低的購電費用'] = 0
                cost_benefit.loc[ind, '降低購電的NG購入成本'] = 0
                cost_benefit.loc[ind, '降低購電的TG維運成本'] = 0
                cost_benefit.loc[ind, '降低購電的碳費'] = 0

        self.update_benefit_tables(cost_benefit, t_resolution, version_used = self.version_used)
        self.trend_chart.plot_from_dataframe(cost_benefit)

        self.statusBar().clearMessage()

    def update_benefit_tables(self, cost_benefit=None, t_resolution=None, version_used=None, initialize_only=False):
        def color_config(name):
            return {
                '減少外購電金額': ('#8064A2', '#DDD0EC', 'white', 'blue'),
                '增加外售電金額': ('#769d64', '#D8E4BC', 'white', 'blue'),
                'NG 發電成本': ('#F79646', '#FBE4D5', 'white', 'red'),
                'TG 維運成本': ('#F79646', '#FBE4D5', 'white', 'red'),
                '總效益': ('#D9D9D9', '#EAF1FA', 'black', None)
            }.get(name, ('#FFFFFF', '#FFFFFF', 'black', 'black'))

        # 加深格線色
        self.tableWidget_4.setStyleSheet("QTableWidget { gridline-color: #666666; }")
        self.tableWidget_5.setStyleSheet("QTableWidget { gridline-color: #666666; }")

        # 表頭與欄寬初始設定
        self.tableWidget_4.setRowCount(5)
        self.tableWidget_4.setColumnCount(2)
        self.tableWidget_4.verticalHeader().setVisible(False)
        self.tableWidget_4.horizontalHeader().setVisible(False)
        self.tableWidget_4.setColumnWidth(0, 120)
        self.tableWidget_4.setColumnWidth(1, 120)
        self.tableWidget_4.verticalHeader().setDefaultSectionSize(28)

        self.tableWidget_5.setRowCount(10)
        self.tableWidget_5.setColumnCount(9)
        self.tableWidget_5.verticalHeader().setVisible(False)
        self.tableWidget_5.horizontalHeader().setVisible(False)

        for col in range(9):
            if col == 0:
                self.tableWidget_5.setColumnWidth(col, 80)
            elif col in [2, 3, 4, 6, 7, 8]:
                self.tableWidget_5.setColumnWidth(col, 90)
            else:
                self.tableWidget_5.setColumnWidth(col, 60)
        self.tableWidget_5.verticalHeader().setDefaultSectionSize(28)

        # 呼叫函式進行tableWidget_5 的表頭設計
        self.set_tableWidget5_header()

        # 🧩 NG 發電成本與 TG 維運成本版本資料（多版本）
        if not initialize_only and version_used and "ng_cost_versions" in version_used:
            cost_tip = self.build_cost_tooltip(version_used["ng_cost_versions"])
            self.tableWidget_5.item(1, 3).setToolTip(cost_tip)
            self.tableWidget_5.item(1, 7).setToolTip(cost_tip)

        # ** 在模擬表頭的tooltip 增加說明 **
        self.tableWidget_5.item(1, 2).setToolTip("減少外購電金額：\n對應時段的總金額")
        self.tableWidget_5.item(1, 4).setToolTip("減少外購電效益：\n金額 - 成本")
        self.tableWidget_5.item(1, 6).setToolTip("增加外售電金額：\n對應時段的總金額")
        self.tableWidget_5.item(1, 8).setToolTip("增加外售電效益：\n金額 - 成本")

        if initialize_only:
            self.tableWidget_4.setRowCount(5)
            self.tableWidget_4.setColumnCount(2)
            items = ['減少外購電金額', '增加外售電金額', 'NG 發電成本', 'TG 維運成本', '總效益']
            for row, name in enumerate(items):
                bg_name, bg_value, fg_name, fg_value = color_config(name)
                self.tableWidget_4.setItem(row, 0,
                                           make_item(name, fg_color=fg_name, bg_color=bg_name, align='center',
                                                          font_size=11))
                self.tableWidget_4.setItem(row, 1, make_item("$0", fg_color=fg_value or 'black', bg_color=bg_value,
                                                                  align='right', font_size=11))
            periods = ['夏尖峰', '夏半尖峰', '夏離峰', '夏週六半', '非夏半尖峰', '非夏離峰', '非夏週六半','小計']
            for i, period in enumerate(periods):
                row = i + 2
                bg = self.get_period_background(period)
                self.tableWidget_5.setItem(row, 0, make_item(period, bg_color=bg))

            self.tableWidget_4.setStyleSheet("QTableWidget { background-color: #FFFFFF; gridline-color: #666666; }")
            self.tableWidget_5.setStyleSheet("QTableWidget { background-color: #FFFFFF; gridline-color: #666666; }")
            self.auto_resize(self.tableWidget_4)
            self.auto_resize(self.tableWidget_5)
            return

        # ===== 資料填入 tableWidget_4 =====
        summary_data = [
            ('減少外購電金額', cost_benefit['降低的購電費用'].sum()),
            ('增加外售電金額', cost_benefit['增加的售電收入'].sum()),
            ('NG 發電成本', cost_benefit['降低購電的NG購入成本'].sum() + cost_benefit['增加售電的NG購入成本'].sum()),
            ('TG 維運成本', cost_benefit['降低購電的TG維運成本'].sum() + cost_benefit['增加售電的TG維運成本'].sum()),
        ]
        total_benefit = summary_data[0][1] + summary_data[1][1] - summary_data[2][1] - summary_data[3][1]
        summary_data.append(('總效益', total_benefit))

        for row, (name, value) in enumerate(summary_data):
            bg_name, bg_value, fg_name, fg_value = color_config(name)
            if name == '總效益':
                fg_value = 'blue' if value >= 0 else 'red'
            self.tableWidget_4.setItem(row, 0, make_item(name, fg_color=fg_name, bg_color=bg_name, align='center',
                                                              font_size=11))
            self.tableWidget_4.setItem(row, 1, make_item(f"${value:,.0f}", fg_color=fg_value, bg_color=bg_value,
                                                              align='right', font_size=11))
            # 套用 NG 發電成本 / TG 維運成本 tooltip
            if name in ["NG 發電成本", "TG 維運成本"] and version_used:
                ng_cost_versions = version_used.get("ng_cost_versions", [])
                tooltip_html = self.build_ng_table4_tooltip(name, ng_cost_versions)
                self.tableWidget_4.item(row, 0).setToolTip(tooltip_html)

        # ===== 表格 5 資料填入（每個時段） =====
        periods = ['夏尖峰', '夏半尖峰', '夏離峰', '夏週六半', '非夏半尖峰', '非夏離峰', '非夏週六半']
        for i, period in enumerate(periods):
            row = i + 2
            pd_data = cost_benefit[cost_benefit['時段'] == period]

            r_data = pd_data[pd_data['降低的購電費用'] > 0]
            rh = len(r_data) * t_resolution / 3600
            ra = r_data['降低的購電費用'].sum()
            rc = r_data['降低購電的NG購入成本'].sum() + r_data['降低購電的TG維運成本'].sum()
            rb = ra - rc

            i_data = pd_data[pd_data['增加的售電收入'] > 0]
            ih = len(i_data) * t_resolution / 3600
            ia = i_data['增加的售電收入'].sum()
            ic = i_data['增加售電的NG購入成本'].sum() + i_data['增加售電的TG維運成本'].sum()
            ib = ia - ic

            bg_color = self.get_period_background(period)
            self.tableWidget_5.setItem(row, 0, make_item(period, bg_color=bg_color))
            self.tableWidget_5.setItem(row, 1, make_item(f"{rh:.1f} hr", bg_color="#DDD0EC"))
            self.tableWidget_5.setItem(row, 2, make_item(f"${ra:,.0f}", fg_color='blue', align='right',
                                                              bg_color="#DDD0EC"))
            self.tableWidget_5.setItem(row, 3,
                                       make_item(f"${rc:,.0f}", fg_color='red', align='right', bg_color="#FBE4D5"))
            # 替代動態顏色判斷，改為統一顏色
            self.tableWidget_5.setItem(row, 4, make_item(f"${rb:,.0f}",
                                                         fg_color='black', bg_color='#EAF1FA', align='right'))

            self.tableWidget_5.setItem(row, 5, make_item(f"{ih:.1f} hr", bg_color="#D8E4BC"))
            self.tableWidget_5.setItem(row, 6, make_item(f"${ia:,.0f}", fg_color='blue', align='right',
                                                              bg_color="#D8E4BC"))
            self.tableWidget_5.setItem(row, 7, make_item(f"${ic:,.0f}", fg_color='red', align='right', bg_color="#FBE4D5"))
            # 替代動態顏色判斷，改為統一顏色
            self.tableWidget_5.setItem(row, 8, make_item(f"${ib:,.0f}",
                                                         fg_color='black', bg_color='#EAF1FA', align='right'))

            # 🔹 建立購電/售電版本清單（避免重複）
            purchase_versions = []
            sale_versions = []

            for idx in r_data.index:
                ver = self.version_info.get(idx, {}).get("unit_price")
                if ver and ver not in purchase_versions:
                    purchase_versions.append(ver)

            for idx in i_data.index:
                ver = self.version_info.get(idx, {}).get("sale_price")
                if ver and ver not in sale_versions:
                    sale_versions.append(ver)

            # 🔹 套用 tooltip
            if purchase_versions:
                tooltip_html = self.build_price_tooltip(period, purchase_versions)
                self.tableWidget_5.item(row, 2).setToolTip(tooltip_html)

            if sale_versions:
                tooltip_html = self.build_price_tooltip(period, sale_versions, is_sale=True)
                self.tableWidget_5.item(row, 6).setToolTip(tooltip_html)

            # ➤ 減少外購電成本 tooltip
            rc_ng = r_data['降低購電的NG購入成本'].sum()
            rc_tg = r_data['降低購電的TG維運成本'].sum()
            self.tableWidget_5.item(row, 3).setToolTip(self.build_cost_cell_tooltip(rc_ng, rc_tg))

            # ➤ 增加外售電成本 tooltip
            ic_ng = i_data['增加售電的NG購入成本'].sum()
            ic_tg = i_data['增加售電的TG維運成本'].sum()
            self.tableWidget_5.item(row, 7).setToolTip(self.build_cost_cell_tooltip(ic_ng, ic_tg))

        # ===== 小計列 =====
        row = len(periods) + 2
        reduce_all = cost_benefit[cost_benefit['降低的購電費用'] > 0]
        increase_all = cost_benefit[cost_benefit['增加的售電收入'] > 0]

        rh = len(reduce_all) * t_resolution / 3600
        ra = reduce_all['降低的購電費用'].sum()
        rc = reduce_all['降低購電的NG購入成本'].sum() + reduce_all['降低購電的TG維運成本'].sum()
        rb = ra - rc

        ih = len(increase_all) * t_resolution / 3600
        ia = increase_all['增加的售電收入'].sum()
        ic = increase_all['增加售電的NG購入成本'].sum() + increase_all['增加售電的TG維運成本'].sum()
        ib = ia - ic

        subtotal = [
            make_item("小計", bold=True, bg_color="#D9D9D9"),
            make_item(f"{rh:.1f} hr", bg_color="#DDD0EC"),
            make_item(f"${ra:,.0f}", fg_color='blue', align='right', bold=True, bg_color="#DDD0EC"),
            make_item(f"${rc:,.0f}", fg_color='red', align='right', bold=True, bg_color="#FBE4D5"),
            make_item(f"${rb:,.0f}", fg_color='blue' if rb >= 0 else 'red', align='right', bold=True,
                           bg_color="#EAF1FA"),
            make_item(f"{ih:.1f} hr", bg_color="#D8E4BC"),
            make_item(f"${ia:,.0f}", fg_color='blue', align='right', bold=True, bg_color="#D8E4BC"),
            make_item(f"${ic:,.0f}", fg_color='red', align='right', bold=True, bg_color="#FBE4D5"),
            make_item(f"${ib:,.0f}", fg_color='blue' if ib >= 0 else 'red', align='right', bold=True,
                           bg_color="#EAF1FA")
        ]
        for col, item in enumerate(subtotal):
            self.tableWidget_5.setItem(row, col, item)

        # ** 計算及顯示指定期間的NG 使用量
        ng_active = cost_benefit[cost_benefit['NG 總用量'] > 0]
        ng_duration_secs = len (ng_active) * t_resolution
        ng_amount = cost_benefit.loc[cost_benefit['NG 總用量']>0, 'NG 總用量'].mean() * ng_duration_secs / 3600
        par1 = get_ng_generation_cost_v2(self.unit_prices, cost_benefit.index[0])
        ng_kwh = ng_amount * par1.get('convertible_power')
        self.label_30.setText(f"{ng_amount:,.0f} Nm3\n({ng_kwh:,.0f} kWH)")
        self.label_30.setStyleSheet("color: #004080; font-size:12pt; font_weight: bold;")
        self.label_30.setToolTip("查詢區間內 NG 總使用量（單位：Nm³）")

        self.auto_resize(self.tableWidget_4)
        self.auto_resize(self.tableWidget_5)

    def set_tableWidget5_header(self):
        # 第一層表頭
        header_row1 = ["時段", "減少外購電", "", "", "", "增加外售電", "", "", ""]
        for col, text in enumerate(header_row1):
            bg = "#ececec" if col == 0 else ("#8064A2" if 1 <= col <= 4 else "#769d64")
            fg = "black" if col == 0 else "white"
            self.tableWidget_5.setItem(0, col, make_item(text, bold=True, bg_color=bg, fg_color=fg))

        # 第二層表頭
        header_row2 = ["時段", "時數", "金額", "成本", "效益", "時數", "金額", "成本", "效益"]
        for col, text in enumerate(header_row2):
            bg_map = {
                1: '#DDD0EC', 2: '#DDD0EC', 3: '#FBE4D5', 4: '#EAF1FA',
                5: '#D8E4BC', 6: '#D8E4BC', 7: '#FBE4D5', 8: '#EAF1FA'
            }
            bg = bg_map.get(col, '#FFFFFF')
            self.tableWidget_5.setItem(1, col, make_item(text, bold=True, bg_color=bg))

        # 合併儲存格
        self.tableWidget_5.setSpan(0, 0, 2, 1)
        self.tableWidget_5.setSpan(0, 1, 1, 4)
        self.tableWidget_5.setSpan(0, 5, 1, 4)

    def get_period_background(self, period):
        color_map = {
            '夏尖峰': '#FFD9B3',
            '夏半尖峰': '#FFE5CC',
            '夏離峰': '#FFF1E0',
            '夏週六半': '#FFF8F0',
            '非夏半尖峰': '#D0E6FF',
            '非夏離峰': '#E3F0FF',
            '非夏週六半': '#F0F8FF',
            '小計': '#D9D9D9'
        }
        return color_map.get(period, '#FFFFFF')

    def get_benefit_colors(self, value) -> Tuple[str, str]:  # 用 typing.Tuple 替代 tuple[str, str]
        return ('blue', '#E6F0FF') if value >= 0 else ('red', '#FBE4E4')

    def build_ng_table4_tooltip(self, name: str, ng_cost_versions: list) -> str:
        """
        根據欄位名稱，產生 NG 發電成本或 TG 維運成本的 tooltip 內容（支援多版本）
        """
        if not ng_cost_versions or name not in ["NG 發電成本", "TG 維運成本"]:
            return ""

        tooltip_lines = [f"{name}："]

        for v in ng_cost_versions:
            if name == "NG 發電成本" and v.get("value") is not None:
                tooltip_lines.append(
                    f"<span style='color:#004080;'>{v['value']:.4f} 元/kWH</span> "
                    f"<span style='color:#999999;'>（適用：{v['start']} ~ {v['end']}）</span>"
                )
            elif name == "TG 維運成本" and v.get("tg_cost") is not None:
                tooltip_lines.append(
                    f"<span style='color:#004080;'>{v['tg_cost']:.4f} 元/kWH</span> "
                    f"<span style='color:#999999;'>（適用：{v['start']} ~ {v['end']}）</span>"
                )

        return (
                "<html><body><div style='white-space:pre; font-size:9pt;'>"
                + "<br>".join(tooltip_lines)
                + "</div></body></html>"
        )

    def build_cost_cell_tooltip(self, ng_cost: float, tg_cost: float) -> str:
        """
        回傳 NG 與 TG 成本組成的 tooltip HTML 文字。
        金額為紅色，格式固定。
        """
        return (
            "<html><body><div style='white-space:pre; font-size:9pt;'>"
            f"NG 發電成本：<span style='color:#C00000;'>${ng_cost:,.0f}</span> 元<br>"
            f"TG 維運成本：<span style='color:#C00000;'>${tg_cost:,.0f}</span> 元"
            "</div></body></html>"
        )

    def build_cost_tooltip(self, ng_cost_list):
        """
        根據版本清單產生減少外購電成本與增加外售電成本的 tooltip。
        支援多版本、HTML 格式與顏色標記。
        """
        if not ng_cost_list:
            return ""

        tooltip_lines = [
            "減少外購電成本：(1) + (2)",
            "<b>(1) NG 發電成本單價：</b>"
        ]

        for ver in ng_cost_list:
            if ver.get("value") is not None:
                tooltip_lines.append(
                    f"<span style='color:#004080;'>{ver['value']:.4f} 元/kWH</span> "
                    f"<span style='color:#999999;'>（適用：{ver['start']} ~ {ver['end']}）</span>"
                )

        tooltip_lines.append("<b>(2) TG 維運成本單價：</b>")
        for ver in ng_cost_list:
            if ver.get("tg_cost") is not None:
                tooltip_lines.append(
                    f"<span style='color:#004080;'>{ver['tg_cost']:.4f} 元/kWH</span> "
                    f"<span style='color:#999999;'>（適用：{ver['start']} ~ {ver['end']}）</span>"
                )

        return (
                "<html><body><div style='white-space:pre; font-size:9pt;'>"
                + "<br>".join(tooltip_lines)
                + "</div></body></html>"
        )

    def build_price_tooltip(self, period, ver_list, is_sale=False):
        if not ver_list:
            return ""

        # 決定表頭名稱
        if is_sale:
            header = "離峰" if period in ['夏離峰', '非夏離峰'] else "非離峰"
        else:
            header = period

        lines = [f"<b>{header}單價：</b>"]

        # 單價列表
        for ver in sorted(ver_list, key=lambda x: x['version']):
            price_str = f"<span style='color:#004080;'>${ver['value']:.4f}</span>"
            range_str = f"<span style='color:#999999;'>（適用：{ver['version']}）</span>"
            lines.append(f"{price_str}{range_str}")

        # 判斷是否為 NG 成本欄位（非欄位本身而是 tooltip 顯示）
        if ver_list and isinstance(ver_list[0], dict):
            first = ver_list[0]

            ng_cost = first.get('ng_cost')
            tg_cost = first.get('tg_cost') or first.get('tg_maintain_cost')
            range_text = ""

            if first.get("ng_cost_range") and isinstance(first["ng_cost_range"], str):
                range_text = first["ng_cost_range"]
            elif first.get("ng_cost_range_text"):
                range_text = first["ng_cost_range_text"]

            if ng_cost and tg_cost:
                lines.append("<hr>")
                lines.append(
                    f"<div style='color:#666666; font-size:8pt;'>"
                    f"NG 發電成本：{ng_cost:.4f} 元/kWh<br>"
                    f"TG 維運成本：{tg_cost:.4f} 元/kWh<br>"
                    f"（適用：{range_text}）"
                    f"</div>"
                )

        return "<html><body><div style='white-space:pre; font-size:9pt;'>" + "<br>".join(lines) + "</div></body></html>"

    def auto_resize(self, table: QtWidgets.QTableWidget, min_height: int = 60):
        """
        自動根據欄寬與 row 數調整 tableWidget 大小
        若為空表格，則高度設為 min_height
        """
        frame = table.frameWidth()

        # 水平 & 垂直 scrollbar 高度
        scroll_w = table.verticalScrollBar().sizeHint().width() if table.verticalScrollBar().isVisible() else 0
        scroll_h = table.horizontalScrollBar().sizeHint().height() if table.horizontalScrollBar().isVisible() else 0

        # 寬度：總欄寬 + 邊框 + scrollbar
        total_w = sum(table.columnWidth(c) for c in range(table.columnCount())) + 2 * frame + scroll_w
        table.setFixedWidth(total_w)

        # 高度：根據是否有 row 調整
        if table.rowCount() == 0:
            table.setFixedHeight(min_height)
        else:
            total_h = table.verticalHeader().length() + table.horizontalHeader().height() + 2 * frame + scroll_h
            table.setFixedHeight(total_h)

if __name__ == "__main__":
    pi_client = PIClient()
    app = QtWidgets.QApplication(sys.argv)
    myWin = MyMainForm()
    myWin.show()
    sys.exit(app.exec())
from logging_utils import setup_logging, log_exceptions, timeit, get_logger

setup_logging("logs/app.log", level="INFO")
logger = get_logger(__name__)

import sys, re, math, time
from typing import Tuple, Optional, List, Protocol, runtime_checkable, cast
import pandas as pd
from PyQt6 import QtCore, QtWidgets, QtGui
from PyQt6.QtGui import QLinearGradient
from UI import Ui_MainWindow
from tariff_version import get_current_rate_type_v6, get_ng_generation_cost_v2, format_range
from make_item import make_item
from visualization import TrendChartCanvas, TrendWindow, plot_tag_trends, PieChartArea, StackedAreaCanvas, GanttCanvas
from ui_handler import setup_ui_behavior
from data_sources.pi_client import PIClient
from data_sources.schedule_scraper import scrape_schedule
from data_sources.data_analysis import analyze_production_avg_cycle, estimate_speed_from_last_peaks
import numpy as np

# 設定全域未捕捉異常的 hook
def handle_uncaught(exc_type, exc_value, exc_traceback):
    # 如果是 Ctrl+C 等 KeyboardInterrupt，就交還給預設行為
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # 否則把完整堆疊與例外都記錄到日誌
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

class DashboardThread(QtCore.QThread):
    """
    在背景固定頻率（預設每 11 秒）呼叫 MainWindow.dashboard_value() 以抓取即時值，
    並透過 sig_pie_series(pd.Series) 把pie chart 要用的 c_values 發送回 **主執行緒**。
    同時呼叫 main_win.make_stacked_frames()，丟回堆疊圖需要的 DataFrame

    特性
    ------
    - 支援 requestInterruption() 平滑中斷
    - 內建例外處理與狀態列（statusBar）訊息
    - 每次循環將 dashboard_value() 的 pd.Series(shape≈226) 發出，用於 pie 圖繪製
    """
    # 新增把資料送回主執行緒的 signal
    sig_pie_series = QtCore.pyqtSignal(object)      # 用來傳 pd.Series (shape=226)
    sig_stack_df = QtCore.pyqtSignal(object)        # 堆疊圖, payload 會是dict

    def __init__(self, main_win, interval: float = 11.0):
        super().__init__(main_win)
        self.main_win = main_win
        self.interval = interval

    def run(self) -> None:
        # 只要沒有被 requestInterruption() 就持續執行
        while not self.isInterruptionRequested():
            try:
                c_values: pd.Series = self.main_win.dashboard_value()
                # 正常才發射給主執行緒
                if isinstance(c_values, pd.Series):
                    self.sig_pie_series.emit(c_values)
            except Exception:
                logger.error("DashboardThread 未捕捉例外", exc_info=True)
                self.main_win.statusBar().showMessage("⚠ 更新即時值失敗，請檢查 PI Server 連線", 0)

            # 2) 新增堆疊圖資料（單位 & 燃料）
            try:
                df_raw = self.main_win.fetch_stack_raw_df()
                frames = self.main_win.make_stacked_frames(df_raw)  # 產出 by_unit / by_fuel / by_unit_detail
                by_unit = frames["by_unit"]
                by_fuel = frames["by_fuel"]  # 已是「NG 固定公式，其餘按剩餘量比例」結果

                # 固定燃料堆疊順序（NG 底、COG 中、MG 上），避免平均值排序
                by_fuel = by_fuel[["NG", "COG", "MG"]]

                # 丟回 UI：你已經有 on_stack_df(payload) 插槽
                last_ts = getattr(self, "_last_stack_ts", None)
                new_ts = None
                if not by_unit.empty:
                    new_ts = by_unit.index[-1]
                if new_ts is not None and new_ts == last_ts:
                    pass
                else:
                    self._last_stack_ts = new_ts
                    self.sig_stack_df.emit({"by": "unit", "df": by_unit})
                    self.sig_stack_df.emit({"by": "fuel", "df": by_fuel})

            except Exception:
                logger.error("DashboardThread 產生堆疊圖資料失敗", exc_info=True)

            # 分段 sleep，以便快速響應中斷
            slept = 0.0
            while slept < self.interval and not self.isInterruptionRequested():
                time.sleep(0.5)
                slept += 0.5
        logger.info("DashboardThread 己收到中斷，停止執行。")

@runtime_checkable
class ScheduleResult(Protocol):
    ok: bool
    reason: Optional[str]
    past: pd.DataFrame
    current: pd.DataFrame
    future: pd.DataFrame
    fetched_at: pd.Timestamp

class ScheduleThread(QtCore.QThread):
    """
    週期性在背景執行緒抓取「製程排程」資料並以訊號回傳主執行緒。

    設計
    ----
    - 只做資料取得（呼叫 `scrape_schedule()`），**不在子執行緒動任何 Qt UI**。
    - 每次取得結果後透過 `sig_schedule`（payload: ScheduleResult）送回主執行緒。
    - 尊重 `requestInterruption()`，以短睡眠片段快速響應停止。

    Signals
    -------
    sig_schedule : object
        Payload 於執行時為符合 ScheduleResult 協議的物件（具有 ok/reason/past/current/future）。

    Parameters
    ----------
    main_win : MyMainWindow
        主視窗，持有 UI 與 slot。
    interval : float, default=30.0
        兩次抓取間隔秒數。
    """
    sig_schedule = QtCore.pyqtSignal(object)        # payload: ScheduleResult
    def __init__(self, main_win: "MyMainWindow", interval: float = 30.0):
        super().__init__(main_win)
        self.main_win = main_win
        self.interval = interval

    def run(self):
        # 只要沒有被 requestInterruption() 就持續執行
        while not self.isInterruptionRequested():
            try:
                res = scrape_schedule()
                self.sig_schedule.emit(res)     # 將爬取的排程資料丟回主執行緒
            except Exception:
                # 記錄log
                logger.error("背景排程發生錯誤", exc_info=True)

            slept = 0.0
            while slept < self.interval and not self.isInterruptionRequested():
                time.sleep(0.5)
                slept += 0.5
        logger.info("ScheduleThread 己收到中斷，停止執行。")

class LoadingOverlay(QtWidgets.QWidget):
    """
        可在特定期間，用來阻止主視窗所有互動
    """
    def __init__(self, parent):
        super().__init__(parent)
        # 讓 overlay 填滿 parent 的 client area
        self.setGeometry(parent.rect())
        # 半透明遮罩
        self.setStyleSheet("background-color: rgba(0, 0, 0, 120);")
        self.setWindowFlags(
            QtCore.Qt.WindowType.Widget |
            QtCore.Qt.WindowType.FramelessWindowHint
        )

        # ----- 中央小視窗 -----
        self.box = QtWidgets.QFrame(self)
        self.box.setFixedSize(220, 100)
        # 自訂底色與透明度 (最後一個值 180 / 255 ≈ 70% 不透明度)
        self.box.setStyleSheet("""
                QFrame {
                    background-color: rgba(240, 240, 240, 180);
                    border-radius: 10px;
                }
                QLabel {
                    font-size: 16px;
                    font_weight: bold;   
                }
            """)
        # 初次定位到中心
        self._center_box()

        # 用垂直 Layout，把文字與 progress bar 都置中
        layout = QtWidgets.QVBoxLayout(self.box)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)  # 整個 layout 元素置中

        # 提示文字
        self.label = QtWidgets.QLabel("查詢中，請稍候…", self.box)
        self.label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.label)

        # 不確定型進度條
        self.bar = QtWidgets.QProgressBar(self.box)
        self.bar.setRange(0, 0)  # 0,0 模式會無限跑馬燈
        self.bar.setFixedHeight(12)
        self.bar.setTextVisible(False)
        layout.addWidget(self.bar)

        self.hide()

    def _center_box(self):
        """把 box 移到 parent 的正中央"""
        pw, ph = self.width(), self.height()
        bw, bh = self.box.width(), self.box.height()
        self.box.move((pw - bw) // 2, (ph - bh) // 2)

    def show(self):
        # 每次顯示前都重新置中
        self.setGeometry(self.parent().rect())
        self._center_box()
        super().show()

    def hide(self):
        super().hide()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # 視窗大小變動時，也要更新 overlay 與 box 位置
        self.setGeometry(self.parent().rect())
        self._center_box()

    def moveEvent(self, event):
        super().moveEvent(event)
        # 視窗被拖動時，也同步
        self._center_box()

class PiReader(QtCore.QThread):
    """
    用於在子執行緒中非同步查詢 PI 資料。

    屬性:
        pi_client (PIClient): 執行 PI 查詢的客戶端實例。
        key (Any): 識別此執行緒結果的唯一鍵。
        query_kwargs (Optional[dict]): 傳遞給 PIClient.query 的參數。
        logger (Logger): 用於記錄狀態與錯誤的 logger。
    """
    # 定義完成訊號，傳回讀到的資料或例外
    data_ready = QtCore.pyqtSignal(object, object) # (tag_group, data 或 exception)
    """當資料查詢完成或發生錯誤時發射。
    參數:
        key (Any): 查詢識別鍵。
        result (object): 查詢結果的 DataFrame，或是 Exception。"""

    def __init__(self, pi_client, key, parent=None):
        """
        初始化 PiReader 執行緒。

        Args:
            pi_client (PIClient): 用於查詢 PI 的客戶端實例。
            key (Any): 識別此執行緒結果的鍵。
            parent (QObject, optional): 父 QObject。
        """

        super().__init__(parent)
        self.pi_client = pi_client
        self.query_kwargs = None    # 先暫時不給參數
        self.key = key
        self.logger = get_logger(__name__)

    def set_query_params(self, **kwargs):
        """
        設定 PIClient.query 的參數，必須在啟動執行緒前呼叫。

        Args:
            **kwargs: 對應 PIClient.query 的參數，例如 st, et, tags, summary, interval, fillna_method。
        """
        self.query_kwargs = kwargs

    @timeit(level=20)
    def run(self):
        if not self.query_kwargs:
            self.logger.error("run(0 前必須先呼叫 set_query_params() 設定參數")
            return

        self.logger.info(f"開始 PI 查詢, 查詢種類:{self.key}")
        try:
            data = self.pi_client.query(**self.query_kwargs)
            self.logger.info("PI 查詢完成，資料筆數: %d", len(data))
            self.data_ready.emit(self.key, data)

        except Exception as e:
            self.logger.exception("PI 查詢失敗")
            self.data_ready.emit(self.key, e)

class MyMainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super(MyMainWindow, self).__init__()
        self.setupUi(self)

        # --- 用QThread 同時讀取兩組PI 資料的功能 (等待放到 ui_handler.py) ---
        self.pi_client = pi_client

        # -------- 從外部資料讀取設定檔，並儲存成這個實例本身的成員變數 -----------
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
        self.hsm_attribute = pd.DataFrame()            # 用來紀錄從HSM 用電資料分析出來的特性
        self._history_results ={}           # 在on_data_ready() 中用來暫存結果 dict
        self._pending_column = None         # 等待更新(update_history_to_tws()) 的欄位key
        self._isFetching = False            # 用來防止重復觸發 history_demand_of_groups 的Guard flag
        self.scheduler_thread: Optional[ScheduleThread] = None      # 當作ScheduleThread 的實例，作為背景排程
        self.dashboard_thread: Optional[DashboardThread] = None     # 當作DashboardThread 的實例，作為背景排程
        self.pie: Optional["PieChartArea"] = None       # 和 pie chart 有關
        self.loader = LoadingOverlay(self)  # 彈出半透明loading 的
        self._styling_in_progress = False

        self.radioButton_5.setChecked(True)  # 支援選擇 KWH 或 P 值的查詢方式 (這個項目要先做)
        self.dashboard_value()
        # 建立趨勢圖元件並加入版面配置
        self.trend_chart = TrendChartCanvas(self)
        setup_ui_behavior(self)

        # --- 等待放到 ui_handler.py (這些都是功能試調區的部份)---
        self.pushButton_6.clicked.connect(self.analyze_hsm)
        self.pushButton_9.clicked.connect(self.on_show_trend)
        #self.pushButton_7.clicked.connect(self.show_gantt_window)

        self.listWidget_2.addItems(['HSM 軋延機組'])
        self.listWidget_2.addItems([str(name) for name in self.tag_list['name']])
        self.listWidget_2.itemDoubleClicked.connect(self.add_target_tag_to_list3)
        self.listWidget_3.itemDoubleClicked.connect(self.remove_target_tag_from_list3)

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

        # 開發測試功能區域初始狀態
        self.develop_option.setChecked(False)
        self.tabWidget.setTabVisible(4, False)

        # ===== 燃氣發電佔比的 pie chart 相關初始化 =====
        self.pie = PieChartArea(self.verticalLayout_2, with_toolbar=False)
        self._last_pie_series: Optional[pd.Series] = None   # 用來暫存最後一筆要給pie chart 用的資料
        self.comboBox.currentIndexChanged.connect(self._on_tg_switch)
        self.tw3_2.itemSelectionChanged.connect(self._on_tw3_2_select) #tw3_2 點選 TG 節點切換

        # ===== stack chart (發電量 by unit 或 by fuel) 相關初始化 =====
        self.canvas_unit = None
        self.canvas_fuel = None

        # --- Gantt chart 初始化：放在 __init__ 內合適位置 ---
        self.canvas_gantt = None

        # 圖表顯示相關設定
        self.checkBox_5.stateChanged.connect(self._apply_chart_mode)    # 是否顯示圖表的選項
        self.comboBox_3.currentIndexChanged.connect(self._apply_chart_mode)     # chart 種類的選擇comboBox
        self.checkBox_5.setChecked(True)
        self.comboBox_3.setCurrentIndex(3)

        try:
            self.tw2_2.setTextElideMode(QtCore.Qt.TextElideMode.ElideRight)
        except Exception:
            pass

    @QtCore.pyqtSlot(object)
    def on_schedule_result(self, res_obj: object) -> None:
        """
        接收背景 ScheduleThread 發出的排程結果（**主執行緒**），並更新 tw4 與 Gantt。

        Parameters
        ----------
        res_obj : object
            執行時 payload 為符合 ScheduleResult 協議的物件；包含：
            - ok : bool
            - reason : Optional[str]
            - past, current, future : pandas.DataFrame
              欄位至少含「開始時間、結束時間、爐號、製程」（current 另含「製程狀態」）。

        Notes
        -----
        - 函式開頭以 typing.cast(ScheduleResult, res_obj) 提示型別檢查器，避免
          “Unresolved attribute reference … for class 'object'” 警告。
        - 本函式內可以安全建立/操作 QWidget（主執行緒）。
        """
        res = cast(ScheduleResult, res_obj)
        self.update_tw4_schedule(res)

        if not hasattr(self, "canvas_gantt") or self.canvas_gantt is None:
            self.canvas_gantt = GanttCanvas()
            self.verticalLayout_5.addWidget(self.canvas_gantt)
        #res.past = self._gantt_fill_start_end(res.past)
        # 繪圖
        self.canvas_gantt.plot(res.past, res.current, res.future)

    def _gantt_fill_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        給 Gantt 用的欄位補齊：
        - 開始時間若為 NaT，且有「實際開始時間」→ 補上
        - 結束時間若為 NaT，且有「實際結束時間」→ 補上
        不改動原本的「實際開始時間／實際結束時間」欄位。
        """
        if df is None or df.empty:
            return df
        out = df.copy()
        # 只在 NaT 時才補；避免覆蓋本來就有的值
        mask_s = out["開始時間"].isna() & out["實際開始時間"].notna()
        mask_e = out["結束時間"].isna() & out["實際結束時間"].notna()
        if mask_s.any():
            out.loc[mask_s, "開始時間"] = out.loc[mask_s, "實際開始時間"]
        if mask_e.any():
            out.loc[mask_e, "結束時間"] = out.loc[mask_e, "實際結束時間"]
        return out

    def start_schedule_thread(self):
        """
        啟動背景「製程排程」執行緒；先連線 signal→slot，再啟動 thread。

        行為
        ----
        - 建立 `ScheduleThread(self, interval=30.0)`。
        - sig_schedule.connect(self.on_schedule_result) 後再 `start()`，
          確保第一筆結果能被主執行緒處理。
        """
        # 建立並儲存 ScheduleThread 實例
        self.scheduler_thread = ScheduleThread(self, interval=30.0)
        self.scheduler_thread.setObjectName("SchedulerThread")

        # start() 之前建立好連線
        self.scheduler_thread.sig_schedule.connect(self.on_schedule_result)
        # 啟動執行緒
        self.scheduler_thread.start()

    def update_tw4_schedule(self, res):
        """
        以階層節點更新 tw4（QTreeWidget）的製程排程清單。

        呈現規則
        --------
        - 第一層：製程種類（EAF, LF1-1, LF1-2；EAFA/EAFB 併入 EAF）。
        - 第二層兩分類：
            * 「生產或等待中」：`res.current` + `res.future`（依開始時間排序）
            * 「過去排程」：`res.past`（依開始時間排序）
        - 資料為空時顯示占位訊息，並將「狀態欄」置中。

        Parameters
        ----------
        res : ScheduleResult
            具備 ok/reason/past/current/future 的結果物件；DataFrame 欄位格式
            與 GanttCanvas.plot(...) 的期望一致。
        """


        if not res.ok:
            self.statusBar().showMessage(f"排程更新失敗:{res.reason}")

        past_df = res.past
        current_df = res.current
        future_df = res.future

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
        self.statusBar().showMessage(f"排程已更新({res.fetched_at:%H:%M:%S})")

        self.update_tw2_2_column2_from_schedule(past_df, current_df, future_df)

    def update_tw2_2_column2_from_schedule(self, past_df: pd.DataFrame, current_df: pd.DataFrame,
                                           future_df: pd.DataFrame):
        """
        依 scrape_schedule() 的結果，將「產線即時狀況」寫入 tw2_2 的 column 2。
        製程對應：EAF(=EAFA/B 合併)、LF1-1、LF1-2。HSM 由 real_time_hsm_cycle() 填入。
        規則：
          1) 尚未開始： Next: HH:MM 開始生產 (X分後)
          2) 正在生產： <爐別>生產中。預計HH:MM結束。Next HH:MM 開始（若無下一爐，省略 Next）
          3) 無排程：   目前未有排程
        """
        if not hasattr(self, "tw2_2") or self.tw2_2 is None or self.tw2_2.columnCount() <= 2:
            return

        MAP = {"HSM": 0, "EAF": 1, "LF1-1": 2, "LF1-2": 3}
        now = pd.Timestamp.now()

        def fmt_hhmm(ts):
            try:
                return pd.Timestamp(ts).strftime("%H:%M")
            except:
                return "--:--"

        def mins_delta(ts):
            try:
                return int((pd.Timestamp(ts) - now).total_seconds() // 60)
            except:
                return None

        def set_status_row(process_name: str, text: str):
            row = MAP.get(process_name)
            if row is None: return
            try:
                item = self._item_at(self.tw2_2, (row,))
                item.setText(2, text)
                item.setToolTip(2, text)  # 長字→滑鼠提示
                # (第 2 點) 這裡順手統一把 column 2 字體縮小一點
                f = item.font(2)        # 取得 column 2 目前字型
                f.setPointSize(10)      # 調整 column 字體大小
                item.setFont(2, f)      # 套用新的字型
            except Exception:
                pass

        def status_for(proc_name: str):
            if proc_name == "EAF":
                active = current_df[current_df["製程"].isin(["EAFA", "EAFB"])].copy()
                future = future_df[future_df["製程"].isin(["EAFA", "EAFB"])].copy()
            else:
                active = current_df[current_df["製程"] == proc_name].copy()
                future = future_df[future_df["製程"] == proc_name].copy()

            # 正在生產
            if not active.empty:
                furnaces = []
                if proc_name == "EAF":
                    for _, r in active.iterrows():
                        furnaces.append("A爐" if r["製程"] == "EAFA" else "B爐" if r["製程"] == "EAFB" else "")
                    furnaces = sorted({f for f in furnaces if f})
                    prefix = ("、".join(furnaces) + " ") if furnaces else ""
                else:
                    prefix = ""
                end = fmt_hhmm(active["結束時間"].max())
                nxt_txt = ""
                if not future.empty:
                    nxt = future.sort_values(by="開始時間").iloc[0]
                    nxt_txt = f" Next {fmt_hhmm(nxt['開始時間'])} 開始"
                return f"{prefix}生產中。預計{end} 結束。" + nxt_txt

            # 尚未開始（最近未來）
            if not future.empty:
                nxt = future.sort_values(by="開始時間").iloc[0]
                hhmm = fmt_hhmm(nxt["開始時間"])
                mins = mins_delta(nxt["開始時間"])
                tail = f" ({mins}分後)" if mins is not None else ""
                return f"Next: {hhmm} 開始生產{tail}"

            # 完全無排程
            return "目前未有排程"

        # 寫回 tw2_2（HSM 仍由 real_time_hsm_cycle() 處理）
        for proc in ("EAF", "LF1-1", "LF1-2"):
            set_status_row(proc, status_for(proc))

    def _reapply_tree_header_styles(self):
        if getattr(self, "_styling_in_progress", False):
            return
        self._styling_in_progress = True
        try:
            widgets = [getattr(self, n, None) for n in ("tw1", "tw2", "tw3", "tw4", "tw1_2", "tw2_2", "tw3_2")]
            for w in widgets:
                if w is None:
                    continue
                try:
                    h = w.header()
                    if w in (getattr(self, "tw3", None), getattr(self, "tw3_2", None)):
                        h.setStyleSheet(
                            "QHeaderView::section { "
                            "background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #0e6499, stop:1 #9fdeab); "
                            "color: white; font-weight: bold; }"
                        )
                    else:
                        h.setStyleSheet(
                            "QHeaderView::section { "
                            "background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #52e5e7, stop:1 #130cb7); "
                            "color: white; font-weight: bold; }"
                        )

                    # (可選) 若你看到 header 字級被改動，這裡鎖定字級，例如 10pt
                    hf = h.font()
                    hf.setPointSize(11)  # 你的既有字級是多少就填多少
                    h.setFont(hf)
                except Exception:
                    pass
        finally:
            self._styling_in_progress = False

    def eventFilter(self, obj, e):
        if isinstance(obj, QtWidgets.QHeaderView):
            if e.type() in (
                    QtCore.QEvent.Type.StyleChange,
                    QtCore.QEvent.Type.PaletteChange,
                    QtCore.QEvent.Type.PolishRequest,
                    QtCore.QEvent.Type.Show,
            ):
                if not getattr(self, "_styling_in_progress", False):
                    QtCore.QTimer.singleShot(0, self._reapply_tree_header_styles)
        return super().eventFilter(obj, e)

    def _apply_chart_mode(self):
        """
        依目前 UI 狀態（checkBox_5、comboBox_3）切換圖表顯示的容器頁籤。

        邏輯
        ----
        - 當 checkBox_5 未勾選時，隱藏整個圖表 Host 與種類下拉（僅 return）。
        - 勾選時顯示 Host 與下拉，並依 comboBox_3 的 index 切換 chartHost 的頁面：
          0=第一頁、1=第二頁、其他=第三頁。

        Notes
        -----
        此函式只負責「顯示/切換哪一個 chart 容器頁籤」，實際資料繪製與更新
        由其他 slot（例如 `on_stack_df`）處理。
        """
        if not self.checkBox_5.isChecked():
            self.chartHost.setVisible(False)
            self.comboBox_3.setVisible(False)
            return
        self.chartHost.setVisible(True)
        self.comboBox_3.setVisible(True)

        mode = self.comboBox_3.currentIndex()
        self.chartHost.setCurrentIndex(mode)

    def fetch_stack_raw_df(self) -> pd.DataFrame:
        """
        從 PI 系統撈取堆疊圖所需的「原始時間序列資料」，並將欄名轉為迴路名稱。

        流程
        ----
        1) 依 tag_list 取得發電機與燃氣相關的 tag  名單。
        2) 設定查詢區間（目前為「現在回推 120 分鐘」到「現在」，8 秒取樣、平均值、前值補）。
        3) 透過 self.pi_client.query(...) 取得資料。
        4) 將回傳 DataFrame 的 columns 從 actual tag 名稱，轉換成後續
           make_stacked_frames() 會使用的「迴路名稱」（例如 2H120、TG1 sCOG 等）。

        Returns
        -------
        pandas.DataFrame
            時間索引為 DatetimeIndex，欄為「迴路名稱」；適合作為 make_stacked_frames() 的輸入。
        """
        tag_reference = self.tag_list.set_index('name').copy()
        generator_tag = tag_reference.loc['2H120':'5KB19', 'tag_name']
        gas_tag = tag_reference.loc['BFG#1':'TG4 sCOG', 'tag_name']
        tags = pd.concat([generator_tag, gas_tag]).tolist()

        et = pd.Timestamp.now().floor('S')
        st = et - pd.offsets.Minute(120)

        df = self.pi_client.query(st=st, et=et, tags=tags,
                                  summary='AVERAGE', interval='8s', fillna_method='ffill')

        # 把 columns 從 tag 轉成你在 make_stacked_frames 會使用的「迴路名稱」
        mask = tag_reference['tag_name'].isin(tags)
        idx = tag_reference.index[mask]
        df.columns = idx.tolist()
        return df

    @QtCore.pyqtSlot(object)
    def on_stack_df(self, payload: dict):
        """
        接收背景執行緒（DashboardThread）送回的堆疊圖資料，負責在主執行緒建立/更新圖表。

        參數
        ----------
        payload : dict
            必含鍵值：
            - "by": str
                "unit" 或 "fuel"。決定繪製「依機組」或「依燃料」的堆疊圖。
            - "df": pandas.DataFrame
                時間索引（DatetimeIndex）＋對應欄位（依 "by" 不同而異）。
                * by == "unit": 欄位預期含 ["TRT", "CDQ", "TG"]
                * by == "fuel": 欄位預期含 ["NG", "COG", "MG"]

        Behavior:
        --------
        - 僅在第一次收到資料時，於對應的容器建立一個 StackedAreaCanvas 並加入版面配置：
            * "unit" → 加到 verticalLayout_4
            * "fuel" → 加到 verticalLayout_3
          後續同類型更新只呼叫 .plot(df) 重畫，不再重複建 canvas。
        - 根據 "by" 設定 self.canvas_unit / self.canvas_fuel 的 mode 與 `colors`，並以固定欄位順序過濾 DataFrame 後繪圖。

        Side Effects
        ------------
        - 可能建立/保存屬性：`self.canvas_unit`、`self.canvas_fuel`
        - 將 FigureCanvas 動態插入對應的 QVBoxLayout

        Notes
        -----
        - 此 slot 會在 GUI 主執行緒執行（建議以 QueuedConnection 連線），避免跨緒 UI 操作。
        - 欄位順序固定（by_unit: TRT→CDQ→TG；by_fuel: NG→COG→MG），缺欄位時自動略過。

        """
        by = payload.get("by")
        df = payload.get("df")
        if df is None or df.empty:
            return

        # —— by_unit —— (固定順序：TRT、CDQ、TGs)
        if by == "unit":
            # 第一次才建立，之後只 .plot(df)
            if not hasattr(self, "canvas_unit") or self.canvas_unit is None:
                self.canvas_unit = StackedAreaCanvas()  # ← 不傳 mode/colors
                self.verticalLayout_4.addWidget(self.canvas_unit)
            
            # 建立後設定屬性，再重畫
            self.canvas_unit.mode = "by_unit"
            self.canvas_unit.colors = {"TRT": "#7e57c2", "CDQ": "#26a69a",
                                       "TG": "#ef5350", "TGs": "#ef5350"}
            cand = ["TRT", "CDQ", "TG", "TGs"]
            cols = [c for c in cand if c in df.columns]
            self.canvas_unit.plot(df[cols])
   
        # —— by_fuel —— (固定順序：NG、COG、MG)
        elif by == "fuel":
            if not hasattr(self, "canvas_fuel") or self.canvas_fuel is None:
                self.canvas_fuel = StackedAreaCanvas()  # ← 不傳 mode/colors
                self.verticalLayout_3.addWidget(self.canvas_fuel)
 
            self.canvas_fuel.mode = "by_fuel"
            self.canvas_fuel.colors = {"NG": "#4E79A7", "COG": "#F28E2B", "MG": "#59A14F"}
            cols = [c for c in ["NG", "COG", "MG"] if c in df.columns]
            self.canvas_fuel.plot(df[cols])


    @log_exceptions()
    @timeit(level=20)
    def compute_stack_area_metrics(self, *_):
        """
        即時查詢 PI 系統近段時間資料，轉成堆疊圖所需三組 DataFrame，並依 UI 狀態繪製。

        Pipeline
        --------
        1) 讀原始資料：
           - 呼叫 self.fetch_stack_raw_df() 取得最近區間（目前為 ~120 分鐘、8 秒粒度）之平均值序列。
           - 以參照表（self.tag_list）將回傳之 tag 欄位轉成後續計算會用的「迴路名稱」。  # 例如 2H120, 4KA18 等
        2) 組裝堆疊框架：
           - frames = self.make_stacked_frames(df) 產生：
             * by_unit_detail : 各 TG、CDQ#、TRT# 的明細功率
             * by_unit        : 彙整後的 ["TG", "CDQ", "TRT"]
             * by_fuel        : 先依（NG 固定公式）計出各 TG 的 NG 發電量；其餘（COG/MG）按 (總發電量−NG) 以自身占比回配
        3) 繪圖（依 UI 下拉選單 comboBox_3）：
           - index == 0 → 建立 `StackedAreaCanvas()`，使用 by_unit，設定建議色票與 tooltip 格式，加入 verticalLayout_4
           - index == 1 → 建立 `StackedAreaCanvas()`，使用 by_fuel，設定建議色票與 tooltip 格式，加入 verticalLayout_3

        Side Effects
        ------------
        - 視 UI 選擇建立並插入新的 FigureCanvas 到對應 layout
        - 不回傳值，結果以 UI 呈現

        See Also
        --------
        - `fetch_stack_raw_df()`：查詢時間區間與欄位轉名
        - `make_stacked_frames()`：by_unit / by_fuel 的計算細節
        """
        df = self.fetch_stack_raw_df()

        # colors 建議
        UNIT_COLORS = {
            "TG": "#6FB1FF",
            "TRT": "#81C784",
            "CDQ": "#FFB74D",
        }
        FUEL_COLORS = {
            "NG": "#64B5F6",
            "COG": "#BA68C8",
            "MG": "#E57373",
        }

        frames = self.make_stacked_frames(df)  # df 為你 query 回來的 30 分鐘資料
        df_unit = frames["by_unit"]
        df_unit_det = frames["by_unit_detail"]
        df_fuel = frames["by_fuel"]

        if self.comboBox_3.currentIndex() == 0:
            # (A) 機組別堆疊
            canvas_unit = StackedAreaCanvas()

            def fmt_unit(ts, series_dict, total_val):
                # 白字＋白框；背景色由最大項自動套用，不需在這裡指定
                lines = [f"{ts:%H:%M}"]
                lines += [f"{k}: {v:,.1f}" for k, v in series_dict.items()]
                lines.append(f"總發電量: {total_val:,.1f}")
                return "\n".join(lines)

            canvas_unit.plot(
                df_unit,
                colors=UNIT_COLORS,
                legend_title="發電機組類別",
                tooltip_fmt=fmt_unit,
            )
            self.verticalLayout_4.addWidget(canvas_unit)
        else:
            # (B) 燃氣別堆疊
            canvas_fuel = StackedAreaCanvas()

            def fmt_fuel(ts, series_dict, total_val):
                lines = [f"{ts:%H:%M}"]
                lines += [f"{k}: {v:,.1f}" for k, v in series_dict.items()]
                lines.append(f"（總計以 TG 加總）: {total_val:,.1f}")
                return "\n".join(lines)

            canvas_fuel.plot(
                df_fuel,
                colors=FUEL_COLORS,
                legend_title="燃氣別",
                tooltip_fmt=fmt_fuel,
            )
            self.verticalLayout_3.addWidget(canvas_fuel)

    def make_stacked_frames(self, df: pd.DataFrame) -> dict:
        """
        由原始迴路資料組裝堆疊圖所需三種資料集：
        - by_unit_detail：每台 TG 與各 CDQ/TRT 單獨欄位（明細）
        - by_unit：依機組類別彙總（TGs、CDQ、TRT）
        - by_fuel：依燃料彙總（NG / COG / MG；已依規則先轉成發電量後等比縮放）

        Parameters
        ----------
        df : pandas.DataFrame
            由 fetch_stack_raw_df() 取得、且欄名為迴路名稱的時間序列資料。

        Returns
        -------
        dict
            {
              "by_unit_detail": DataFrame,  # 欄位：TG1~TG4、CDQ#1~#2、TRT#1~#2
              "by_unit": DataFrame,         # 欄位：TG、CDQ、TRT
              "by_fuel": DataFrame          # 欄位：NG、COG、MG（單位 MW）
            }

        Notes
        -----
        - by_fuel 係呼叫 compute_by_fuel_power() 依「先轉電力、再以 remain 等比縮放」規則計算。
        - 缺失欄位以 0.0 補齊，確保可穩定相加。
        """
        idx = df.index

        # ---- 機組單位 ----
        tg1 = self._safe_col(df, "2H120") + self._safe_col(df, "2H220")
        tg2 = self._safe_col(df, "5H120") + self._safe_col(df, "5H220")
        tg3 = self._safe_col(df, "1H120") + self._safe_col(df, "1H220")
        tg4 = self._safe_col(df, "1H320") + self._safe_col(df, "1H420")
        cdq1 = self._safe_col(df, "4H120")
        cdq2 = self._safe_col(df, "4H220")
        trt1 = self._safe_col(df, "4KA18")
        trt2 = self._safe_col(df, "5KB19")

        by_unit_detail = pd.DataFrame({
            "TG1": tg1, "TG2": tg2, "TG3": tg3, "TG4": tg4,
            "CDQ#1": cdq1, "CDQ#2": cdq2,
            "TRT#1": trt1, "TRT#2": trt2,
        }, index=idx).fillna(0.0)

        by_unit = pd.DataFrame(index=idx)
        by_unit["TG"] = by_unit_detail[["TG1", "TG2", "TG3", "TG4"]].sum(axis=1, min_count=1)
        by_unit["CDQ"] = by_unit_detail[["CDQ#1", "CDQ#2"]].sum(axis=1, min_count=1)
        by_unit["TRT"] = by_unit_detail[["TRT#1", "TRT#2"]].sum(axis=1, min_count=1)
        by_unit = by_unit.fillna(0.0)

        # ★ 以發電量比例換算的燃料堆疊
        by_fuel = self.compute_by_fuel_power(df, by_unit_detail)

        return {
            "by_unit_detail": by_unit_detail,
            "by_unit": by_unit,
            "by_fuel": by_fuel,  # ← 這個就是你要「先轉成發電量再堆疊」的結果
        }

    def _safe_col(self, df: pd.DataFrame, name: str) -> pd.Series:
        """
        以安全方式取出指定欄位；若欄位不存在，回傳對齊 index 的 0.0 Series。

        Parameters
        ----------
        df : pandas.DataFrame
            來源資料表。
        name : str
            欲擷取之欄名。

        Returns
        -------
        pandas.Series
            若欄位存在則為原欄位資料；否則回傳與 df.index 對齊、值為 0.0 的 Series。
        """
        return df[name] if name in df.columns else pd.Series(0.0, index=df.index)

    def _sum_known_cols(self, df: pd.DataFrame, cols: List[str]) -> pd.Series:
        """
        將 df 中存在於 cols 清單裡的欄位逐一轉 float 後相加；若沒有任何存在欄位，回傳 0.0 Series。

        Parameters
        ----------
        df : pandas.DataFrame
            來源資料表。
        cols : List[str]
            欲相加之欄名清單（可能含不存在欄）。

        Returns
        -------
        pandas.Series
            加總結果；若 cols 都不存在於 df.columns，回傳 0.0 Series（index 與 df 對齊）。
        """
        found = [df[c].astype(float) for c in cols if c in df.columns]
        return sum(found, start=pd.Series(0.0, index=df.index)) if found else pd.Series(0.0, index=df.index)

    def _fuel_flow_sum(self, df: pd.DataFrame, tg_no: int, fuel: str) -> pd.Series:
        """
        依統一命名規則彙總單一 TG 的燃氣流量/熱量欄位（大小寫、空白、黏寫都予以兼容）。

        規則
        ----
        - NG : `sNG + NG`（容許 "TGx sNG" / "TGxSNG" / "TGx NG" / "TGxNG"）
        - COG: sCOG + COG
        - MG : `Mix + sMG + MG`（包含 "TGx Mix/MIX", "TGx sMG/SMG", "TGx MG/MG"）

        Parameters
        ----------
        df : pandas.DataFrame
            來源資料表。
        tg_no : int
            TG 編號（1~4）。
        fuel : str
            "NG"、"COG"、"MG" 其一（大小寫不拘）。

        Returns
        -------
        pandas.Series
            指定 TG + fuel 的總流量/熱量序列；若 fuel 無法辨識，回傳 0.0 Series。
        """
        fn = fuel.upper()
        if fn == "NG":
            variants = [f"TG{tg_no} sNG", f"TG{tg_no}SNG", f"TG{tg_no} NG", f"TG{tg_no}NG"]
        elif fn == "COG":
            variants = [f"TG{tg_no} sCOG", f"TG{tg_no}SCOG", f"TG{tg_no} COG", f"TG{tg_no}COG"]
        elif fn == "MG":
            variants = [
                f"TG{tg_no} Mix", f"TG{tg_no}MIX",
                f"TG{tg_no} sMG", f"TG{tg_no}SMG",
                f"TG{tg_no} MG", f"TG{tg_no}MG",
            ]
        else:
            return pd.Series(0.0, index=df.index)
        return self._sum_known_cols(df, variants)

    def compute_by_fuel_power(self, df: pd.DataFrame, by_unit_detail: pd.DataFrame) -> pd.DataFrame:
        """
        依「先換算成電力、再等比縮放」規則，將各燃氣（NG/COG/MG）的使用量轉成對應發電量。

        規則簡述
        --------
        1) 依 _pie_common_factors 同源參數（熱值、steam_power 等）推導「流量→MW」係數。
           - NG/COG：使用固定熱值
           - MG    ：用動態混氣熱值（由 BFG 與 LDG 當下比例計算）
        2) 先個別換算：`ng_power = ng_flow * ng_k`、`cog_raw = cog_flow * cog_k`、`mg_raw = mg_flow * mg_k`
        3) 計算 `remain = TG_total(MW) - ng_power`（<0 視為 0）
        4) COG/MG 以「各自原始電力占比」縮放到 `remain`：
           - `scale = remain / (cog_raw + mg_raw)`（分母=0 時令 scale=0）
           - `cog = cog_raw * scale`、`mg = mg_raw * scale`
        5) 回傳 DataFrame 欄固定為 `['NG', 'COG', 'MG']`（單位：MW）。

        Parameters
        ----------
        df : pandas.DataFrame
            原始迴路資料（含各 TG 的 sNG/NG、sCOG/COG、Mix/sMG/MG、以及 BFG#?、LDG Input 等欄）。
        by_unit_detail : pandas.DataFrame
            機組明細功率（欄：TG1~TG4、CDQ#1~#2、TRT#1~#2），用來取得 `TG_total(MW)`。

        Returns
        -------
        pandas.DataFrame
            index 與 df 對齊；欄位為 `['NG', 'COG', 'MG']`，值為各時點的發電量（MW）。

        Notes
        -----
        - 本實作符合你「NG 固定公式；COG/MG 依 (總發電量 − NG發電量) 後的比例換算」的最新需求。
        - 內部對於缺失欄位會以 0.0 補齊，並避免除以 0 所致的 NaN/Inf。
        """
        idx = df.index

        # --- 0) 熱值/轉換參數（與 _pie_common_factors 相同來源） ---
        calorics = get_ng_generation_cost_v2(self.unit_prices)
        # 常數（kJ/Nm³、kW per (kJ/s) → 你的專案封裝為 steam_power）
        ng_heat = float(calorics.get('ng_heat', 0.0))
        cog_heat = float(calorics.get('cog_heat', 0.0))
        bfg_heat = float(calorics.get('bfg_heat', 0.0))
        ldg_heat = float(calorics.get('ldg_heat', 0.0))
        steam_pw = float(calorics.get('steam_power', 1.0))  # 避免除 0

        # --- 1) 各燃氣流量（Nm³/h）---
        # NG = Σ(TGx sNG + TGx NG)；COG = Σ(TGx sCOG + TGx COG)；MG = Σ(TGx Mix/MG/sMG)
        def fuel_sum(fuel):
            s = pd.Series(0.0, index=idx)
            for tg_no in (1, 2, 3, 4):
                s = s.add(self._fuel_flow_sum(df, tg_no, fuel).astype(float), fill_value=0.0)
            return s

        ng_flow = fuel_sum("NG")
        cog_flow = fuel_sum("COG")
        mg_flow = fuel_sum("MG")

        # --- 2) 逐時點的動態 Mix 熱值：mix_heat = (BFG_sum*bfg_heat + LDG*ldg_heat) / (BFG_sum + LDG) ---
        # 欄位若缺失就當 0（比照你程式一貫風格）
        bfg_cols = [c for c in df.columns if c.startswith('BFG#')]
        bfg_sum = df[bfg_cols].astype(float).sum(axis=1) if bfg_cols else pd.Series(0.0, index=idx)
        ldg_in = df['LDG Input'].astype(float) if 'LDG Input' in df.columns else pd.Series(0.0, index=idx)
        denom = (bfg_sum + ldg_in).replace(0.0, np.nan)  # 先設 NaN 以便除法，稍後再填回 0
        mix_heat = (bfg_sum * bfg_heat + ldg_in * ldg_heat) / denom
        mix_heat = mix_heat.fillna(0.0)

        # --- 3) 依 _pie_common_factors 的換算公式取得「流量→MW」係數（Nm³/h → MW）---
        # factor = heat / steam_power / 1000   （注意：factor 已含熱值，不可再乘熱值）  ← 這段來自原註解
        # NG/COG 是固定熱值；MG 用動態混氣熱值
        ng_k = ng_heat / steam_pw / 1000.0
        cog_k = cog_heat / steam_pw / 1000.0
        mg_k = mix_heat / steam_pw / 1000.0  # 這是「Series」，每個時間點不同

        # --- 4) 先各自換 MW ---
        ng_power = (ng_flow * ng_k)  # Series
        cog_raw = (cog_flow * cog_k)  # Series
        mg_raw = (mg_flow * mg_k)  # Series

        # --- 5) remain 與等比縮放 ---
        tg_total_power = (
            by_unit_detail[["TG1", "TG2", "TG3", "TG4"]]
            .sum(axis=1, min_count=1).astype(float)
            .reindex(idx).fillna(0.0)
        )
        remain = (tg_total_power - ng_power).clip(lower=0.0)
        denom2 = (cog_raw + mg_raw)

        with np.errstate(divide='ignore', invalid='ignore'):
            scale = np.where(denom2 > 0, remain / denom2, 0.0)
        scale = pd.Series(scale, index=idx)

        cog_power = (cog_raw * scale).fillna(0.0)
        mg_power = (mg_raw * scale).fillna(0.0)

        out = pd.DataFrame({
            "NG": ng_power.clip(lower=0.0).astype(float),
            "COG": cog_power.clip(lower=0.0).astype(float),
            "MG": mg_power.clip(lower=0.0).astype(float),
        }, index=idx)

        # 固定堆疊順序：NG(下) → COG(中) → MG(上)
        cols = [c for c in ["NG", "COG", "MG"] if c in out.columns]
        return out[cols]

    @log_exceptions()
    @QtCore.pyqtSlot(object)
    def _on_pie_series(self, c_values: pd.Series):
        """
        接收 DashboardThread 以 sig_pie_series 傳回的即時資料（單筆 pd.Series），
        於 **主執行緒** 計算 pie 圖所需指標並重繪圖表。

        Parameters
        ----------
        c_values : pandas.Series
            由 dashboard_value() 組成的單筆即時資料。索引需至少包含：
            - 燃氣來源流量（Nm³/h）：
                'TG1 NG'~'TG4 sNG'、'TG1 COG'~'TG4 sCOG'、'TG1 Mix'~'TG4 Mix'
            - 動態混氣熱值來源（Nm³/h）：
                'BFG#1'~'BFG#2'、'LDG Input'
            - 四台 TG 的實際發電量（MW）：
                '2H120'~'2H220'、'5H120'~'5H220'、'1H120'~'1H220'、'1H320'~'1H420'

        Notes
        -----
        - 這個槽以 QueuedConnection 連結，會在 **GUI 主執行緒** 執行；
          請勿在背景執行緒直接操作 PieChartArea 或任何 Qt 元件。
        - _on_tg_switch() 中的 compute_pie_metrics() 僅做加總與四則運算（輕量），
          以目前 11 秒一次、約 234 欄位的負載在主執行緒執行是安全的。

        Side Effects
        ------------
        透過comboBox 的 index tw3_2 的點選，指定要顯示的範圍 (TG1~TG4、TG1、TG2、TG3、TG4)
        最終呼叫 self.pie.update_from_metrics(...) 重繪 verticalLayout_2 裡的甜甜圈圖。
        """
        if not isinstance(c_values, pd.Series):
            return

        # 將接收到的即時資料存在 self._last_pie_series，供其圖表切換時使用。
        self._last_pie_series = c_values
        self._on_tg_switch(idx = self.comboBox.currentIndex())

    def _on_tg_switch(self, idx: int):
        """
        接收 comboBox 的 index 計算 pie 圖所需指標，
        呼叫 self.pie.update_from_metrics(...) 重繪 verticalLayout_2 裡的甜甜圈圖。

        Parameters
        ----------
        idx : int
            - 0:
                TG1 ~ TG4 加總
            - 1~4:
                分別對應TG1、TG2、TG3、TG4
        """
        #
        if self.checkBox_4.isChecked():
            show_ring = True
        else:
            show_ring = False
        
        if idx == 0:
            metrics = self.compute_pie_metrics(self._last_pie_series)
            self.pie.update_from_metrics(
                flows=metrics["flows"],
                est_power=metrics["est_power"],
                real_total=metrics["real_total"],
                tg_count=metrics["tg_count"],
                order=('NG', 'MG', 'COG'),
                show_diff_ring=show_ring,
                title = None,
                #title=f"TG1~TG4 燃料發電比例",
            )
        else:
            tg_no = idx
            tg_name = f"TG{idx}"
            if not hasattr(self, "_last_pie_series"):
                return
            #title = f'TG{tg_no} 燃料發電比例'
            title = None
            metrics = self.compute_pie_metrics_by_tg(self._last_pie_series, tg_no)
            if metrics.get('inactive'):
                self.pie.render_inactive(title=title, message="未運轉 / 無資料")
            else:
                self.pie.set_show_diff_ring(show_ring) # 這個部份後續可能會有選項勾選與否，與現況未即時同步的情況
                self.pie.update_from_metrics(flows = metrics['flows'],
                                             est_power = metrics['mw_est'],
                                             real_total = metrics ['mw_real'],
                                             tg_count = 1,
                                             title = title,
                                             group_label = tg_name)

    def _on_tw3_2_select(self):
        """
        當使用者在 QTreeWidget (tw3_2) 中選取節點時觸發的槽函式。
        ** 後續依UI 介面調整，視情況新增內容 **
        功能：
            - 取得使用者點選的文字（例如 "TG2"）。
            - 驗證文字是否符合 "TG" + 數字 的格式。
            - 若為有效的 TG 編號 (1~4)，則根據最後一次接收到的即時數據
              (self._last_pie_series)，計算該台 TG 的燃料發電比例。
            - 更新 PieChartArea 圖表的標題與內容。

        條件：
            - 必須已經存在屬性 self._last_pie_series，否則不執行更新。
            - 僅接受 "TGs" ~ "TG4" 的選項。

        UI 效果：
            - 圖表標題會顯示為 "TGx 燃料發電比例"。
            - 圖表內容則更新為對應 TG 的燃料發電比例。
        """

        items = self.tw3_2.selectedItems()
        if not items:
            return
        text = items[0].text(0).strip().upper()  # 例如 "TG2"
        if text.startswith("TG"):
            if self.checkBox_4.isChecked():
                show_ring = True
            else:
                show_ring = False
            self.pie.set_show_diff_ring(show_ring)  # 這個部份後續可能會有選項勾選與否，與現況未即時同步的情況

            if text[2:].isdigit():
                tg_no = int(text[2:])
                if 1 <= tg_no <= 4 and hasattr(self, "_last_pie_series"):
                    metrics = self.compute_pie_metrics_by_tg(self._last_pie_series, tg_no)
                    #title = f"TG{tg_no} 燃料發電比例"
                    title = None
                    if metrics.get('inactive'):
                        self.pie.render_inactive(title=title, message="未運轉 / 無資料")
                    self.pie.set_title(title) if hasattr(self, "pie") else None
                    self.pie.update_from_metrics(flows = metrics['flows'],
                                                 est_power = metrics['mw_est'],
                                                 real_total = metrics ['mw_real'],
                                                 tg_count = 1,
                                                 title = title,
                                                 group_label=text,
                                                 )
            else:
                metrics = self.compute_pie_metrics(self._last_pie_series)
                self.pie.update_from_metrics(
                    flows=metrics["flows"],
                    est_power=metrics["est_power"],
                    real_total=metrics["real_total"],
                    tg_count=metrics["tg_count"],
                    order=('NG', 'MG', 'COG'),
                    show_diff_ring=show_ring,
                    title=None,
                    #title=f"TG1~TG4 燃料發電比例",
                )

    def compute_pie_metrics(self, value: pd.Series) -> dict:
        """
        將 dashboard_value() 產生的單筆即時資料（pandas.Series）轉為 pie 圖所需三組數據：
        1) 三種燃氣的總流量（flows, Nm³/h）
        2) 三種燃氣推估的發電量（est_power, MW）
        3) 四台 TG 的實際總發電量（real_total, MW）

        Formulas
        --------
        - 推估發電量（MW）：
            est_power[k] = max(0, flows[k] * factor_k)
        - 實際總發電量（MW）：
            real_total = sum(TG1..TG4 的實際量)

        Parameters
        ----------
        value : pandas.Series
            由 dashboard_value() 整併的即時資料，索引需包含（缺值會當作 0 處理）：
            - 各 TG 之燃氣流量（Nm³/h）：
                'TG1 NG'~'TG4 sNG'、'TG1 COG'~'TG4 sCOG'、'TG1 Mix'~'TG4 Mix'
            - 各 TG 的實際發電量（MW）：
                '2H120'~'2H220'、'5H120'~'5H220'、'1H120'~'1H220'、'1H320'~'1H420'
        Returns
        -------
        dict
            - flows : Dict[str, float]
                三種燃氣（'NG','COG','MG'）的總流量（Nm³/h）。
            - est_power : Dict[str, float]
                依流量與熱值/轉換電力推估的各燃氣發電量（MW）。
            - real_total : float
                四台 TG 的實際總發電量（MW）。
        """

        def get_sum(idx_or_slice):
            try:
                sub = value.loc[idx_or_slice]
            except Exception:
                return 0.0
            if isinstance(sub, pd.Series):
                return float(pd.to_numeric(sub, errors="coerce").fillna(0).sum())
            return float(sub) if pd.notna(sub) else 0.0

        dynamic_mix_heat, ng_k, mg_k, cog_k, calorics = self._pie_common_factors(value)

        # 三種燃氣流量合計（Nm³/h）
        flows = {
            "NG": get_sum(slice('TG1 NG', 'TG4 sNG')),
            "COG": get_sum(slice('TG1 COG', 'TG4 sCOG')),
            "MG": get_sum(slice('TG1 Mix', 'TG4 Mix')),
        }

        # 估算 MW
        est_power = {
            "NG": max(0.0, flows["NG"] * ng_k),
            "COG": max(0.0, flows["COG"] * cog_k),
            "MG": max(0.0, flows["MG"] * mg_k),
        }

        # 實際總 MW
        tg1_real = get_sum(slice('2H120', '2H220'))
        tg2_real = get_sum(slice('5H120', '5H220'))
        tg3_real = get_sum(slice('1H120', '1H220'))
        tg4_real = get_sum(slice('1H320', '1H420'))
        real_total = tg1_real + tg2_real + tg3_real + tg4_real

        # ★ 估算目前運轉的 TG 數量（>1.0 MW 視為運轉中，可自行調門檻）
        tg_vals = [tg1_real, tg2_real, tg3_real, tg4_real]
        tg_count = int(sum(1 for v in tg_vals if float(v) > 1.0))

        return {
            "flows": flows,
            "est_power": est_power,
            "real_total": real_total,
            "tg_count": tg_count,
        }

    def _pie_common_factors(self, value: pd.Series):
        """
        計算換算係數與動態混氣熱質，供 compute_pie_metrics()、compute_pie_metrics_by_tg() 計算熱值時使用
        Formulas
        --------
        - 動態混氣熱值（kJ/Nm³）：
            mix_heat = (BFG_total*bfg_heat + LDG_in*ldg_heat) / (BFG_total + LDG_in)
            若分母為 0，則 mix_heat = 0。
        - 流量換算係數（Nm³/h → MW）：
            factor = heat / steam_power / 1000
            （注意：`factor` 已含「熱值」，**後續不可再乘熱值一次**）
        Notes
        -----
        - 熱值與蒸氣轉換電力由 get_ng_generation_cost_v2(self.unit_prices) 取得，
          需包含鍵：'ng_heat','cog_heat','ldg_heat','bfg_heat','steam_power'。
        - 缺少的索引或 NaN 會視為 0，不會拋例外，方便即時更新流程。

        Parameters
        ----------
        value : pandas.Series
            由 dashboard_value() 整併的即時資料，索引需包含（缺值會當作 0 處理）：
            - 混氣熱值來源（Nm³/h）：
                'BFG#1'~'BFG#2'、'LDG Input'
        Returns
        -------
          dynamic_mix_heat      : 依 BFG/LDG 動態估的混氣熱質
          ng_to_power_factor    : NG 流量 -> MW 的係數
          mg_to_power_factor    : MG 流量 -> MW 的係數（含 dynamic_mix_heat）
          cog_to_power_factor   : COG 流量 -> MW 的係數
          calorics              : 原 get_ng_generation_cost_v2(self.unit_prices) 結果（日後若要取其它欄位會用到）
        """
        calorics = get_ng_generation_cost_v2(self.unit_prices)

        # 動態 MG 熱質（
        bfg_sum = value.loc['BFG#1':'BFG#2'].sum()
        ldg_in = value.loc['LDG Input']
        denom = value.loc['BFG#1': 'LDG Input'].sum()
        dynamic_mix_heat = (bfg_sum * calorics['bfg_heat'] + ldg_in * calorics['ldg_heat']) / max(denom, 1e-9)

        ng_to_power_factor = calorics['ng_heat'] / calorics['steam_power'] / 1000.0
        mg_to_power_factor = dynamic_mix_heat / calorics['steam_power'] / 1000.0
        cog_to_power_factor = calorics['cog_heat'] / calorics['steam_power'] / 1000.0

        return dynamic_mix_heat, ng_to_power_factor, mg_to_power_factor, cog_to_power_factor, calorics

    def compute_pie_metrics_by_tg(self, value: pd.Series, tg_no: int) -> dict:
        """
        依 TG 編號（1~4）回傳該 TG 的三種燃氣發電量與流量、以及「實際發電量」（該 TG 的實績）。
        Parameters
        ----------
        value : pandas.Series
            - 各 TG 之燃氣流量（Nm³/h）：
                'TG1 NG'~'TG4 sNG'、'TG1 COG'~'TG4 sCOG'、'TG1 Mix'~'TG4 Mix'
            - 各 TG 的實際發電量（MW）：
                '2H120'~'2H220'、'5H120'~'5H220'、'1H120'~'1H220'、'1H320'~'1H420'
        Returns
        -------
        {
            'flows': {'NG': float, 'COG': float, 'MG': float},
            'mw_est': {'NG': float, 'COG': float, 'MG': float},
            'mw_real': float
            'inactive': boolen, 用來判斷該TG是否否有運轉
        }
        """
        if tg_no not in (1, 2, 3, 4):
            raise ValueError(f"tg_no must be 1~4, got {tg_no}")

        # 共用係數
        dynamic_mix_heat, ng_k, mg_k, cog_k, _ = self._pie_common_factors(value)

        # --- 該 TG 的三種氣體流量 ---
        ng_flow = float(value.loc[slice(f'TG{tg_no} NG', f'TG{tg_no} sNG')].sum())
        cog_flow = float(value.loc[slice(f'TG{tg_no} COG', f'TG{tg_no} sCOG')].sum())
        mg_flow = float(value.loc[f'TG{tg_no} Mix'])

        # --- 流量 -> 估算 MW ---
        ng_mw = ng_flow * ng_k
        cog_mw = cog_flow * cog_k
        # MG 需乘動態熱質
        mg_mw = mg_flow * mg_k

        # --- 實際發電量（該 TG 的實測） ---
        real_map = {
            1: ('2H120', '2H220'),
            2: ('5H120', '5H220'),
            3: ('1H120', '1H220'),
            4: ('1H320', '1H420'),
        }
        a, b = real_map[tg_no]
        mw_real = float(value.loc[a:b].sum())
        
        eps = 1e-6
        inactive = (
                float(ng_flow) <= eps and
                float(cog_flow) <= eps and
                float(mg_flow) <= eps and
                float(mw_real) <= eps
        )
       
        return {
            'flows': {'NG': ng_flow, 'COG': cog_flow, 'MG': mg_flow},
            'mw_est': {'NG': ng_mw, 'COG': cog_mw, 'MG': mg_mw},
            'mw_real': mw_real,
            'inactive': inactive,
        }

    def develop_option_event(self):
        """
        menu_bar 的開發測試功能被 trigger 時的動作
        """
        if self.develop_option.isChecked():
            self.tabWidget.setTabVisible(4, True)
        else:
            self.tabWidget.setTabVisible(4, False)

    def start_dashboard_thread(self):
        """
        用來建立繼承自 QThread 的 DashboardThread 的實例。
        並定期執行 dashboard_value() 從PI 系統讀取即時值，並更新到指定表格
        Returns:
            None
        """
        # 若已存在先關掉（避免重複連線）
        if getattr(self, "dashboard_thread", None) and self.dashboard_thread.isRunning():
            self.dashboard_thread.requestInterruption()
            self.dashboard_thread.wait(2000)

        # 建立並儲存 DashboardThread 的實例
        self.dashboard_thread = DashboardThread(self, interval=11.0)
        self.dashboard_thread.setObjectName("DashboardThread")
        # 連線都在 start() 之前做，且一次連齊
        self.dashboard_thread.sig_pie_series.connect(self._on_pie_series,
                                                     QtCore.Qt.ConnectionType.QueuedConnection)
        self.dashboard_thread.sig_stack_df.connect(self.on_stack_df, QtCore.Qt.ConnectionType.UniqueConnection)

        # 啟動執行緒
        self.dashboard_thread.start()

    def real_time_hsm_cycle(self):
        """
        近 15 分鐘估算 HSM 生產狀態並顯示四階段文字：
        暫停生產 → （偵測到第一個峰）→ 開始生產，計算速度及秏能中… → （兩峰以上且算得出數值）→ x.x 卷/15分鐘 (約 x.xx MW/卷) → （B>420s）→ 暫停生產
        """
        tag_reference = self.tag_list.set_index('name').copy()
        hsm_tags = tag_reference.loc['9H140':'9KB33', 'tag_name'].tolist()

        et = pd.Timestamp.now().floor('S')
        st = et - pd.offsets.Minute(15)

        df2 = pi_client.query(st=st, et=et, tags=hsm_tags, summary='AVERAGE', interval='5s', fillna_method='ffill')

        power = df2.sum(axis=1)
        pfilter = df2.loc[:, 'W511_HSM/33KV/9H_160/P':'W511_HSM/33KV/9H_170/P'].sum(axis=1)

        r = estimate_speed_from_last_peaks(power=power, threshold=10.0, power_filter=pfilter,
                                           smooth_window=8, prominence=1)

        production_normal = bool(r.get('production_normal'))
        peaks = r.get('peak_times') or []
        A = r.get('A_sec')
        B = r.get('B_sec')
        curr = r.get('current_rate_items_per_15min')
        mw_item = r.get('mw_per_item')

        # 4-state 決策：
        if (not peaks) or (B is not None and B > 420) or (isinstance(curr, (int, float)) and curr == 0):
            text = "暫停生產中"
        elif (len(peaks) == 1) or (not production_normal) or (mw_item is None):
            text = "HSM 開始生產，計算速度及秏能中..."
        elif production_normal and (mw_item is not None) and (curr is not None):
            text = f"{curr:.1f} 卷/15分鐘 (約 {mw_item:.2f} MW/卷)"
        else:
            text = "暫停生產中"

        # 寫入 tw2_2：row=0 假定為 HSM，col=2 為「產線即時狀況」
        try:
            item = self._item_at(self.tw2_2, (0,))
            item.setText(2, text)
            item.setToolTip(2, text)  # 長字顯示完整

            font = item.font(2)       # 取得第二欄目前字型
            font.setPointSize(9)      # 調整字體大小
            item.setFont(2, font)     # 套用新的字型
        except Exception:
            pass

    def remove_target_tag_from_list3(self, item: QtWidgets.QListWidgetItem):
        """
        用來移除功能試調區中, "選擇要顯示(listWidget_3)" 的項目

        Args:
            item: QtWidgets.QListWidgetItem (type): 參數說明。
        Returns:
            type: 回傳值說明。
        """
        row = self.listWidget_3.row(item)           # 取得該 item 所在的列號
        taken = self.listWidget_3.takeItem(row)     # 從listWidget_3 拿出(並移除) 該item
        del taken                                   # del 掉這些物件，避免記憶體累積和洩漏

    def add_target_tag_to_list3(self, item: QtWidgets.QListWidgetItem):
        """
        用來新增項目到功能試調區中的"選擇要顯示(listWidget_3)"

        Args:
            item: QtWidgets.QListWidgetItem (type): 參數說明。
        Returns:
            type: 回傳值說明。
        """
        name = item.text()
        self.listWidget_3.addItems([name])

    @QtCore.pyqtSlot(object, object)
    def on_data_ready(self, tags: tuple, result: object):
        """
        背景查詢完成時的槽函式。接收 PiReader 執行緒帶回的結果，當兩組查詢（all_product_line 與 hsm）
        都完成後，彙整各設備群組的 15 分鐘平均用電，並對 HSM 進行每 15 分鐘的生產週期分析，最後更新 UI。

        參數：
            tags (Any):
                由執行緒回傳的識別資訊。
            result (object):
                成功時為 pd.DataFrame；失敗時為 Exception。

        行為：
            1) 若 result 為 Exception，彈出錯誤對話框並結束。
            2) 成功時暫存至 self._history_results[key]。
            3) 當 thread1、thread2 兩組結果都到齊：
               a. 依 self.tag_list 建立各群組的時間表，將 kWh 轉為 MW/15min（×4），
                  以 Group1/Group2 聚合，產出 self.history_datas_of_groups。
               b. 從 HSM 相關欄位計算主線功率（original_date）與濾除訊號（filter_date），
                  以 15T 切窗後逐窗呼叫 analyze_production_avg_cycle(...) 估算生產件數、每件耗電等指標。
               c. 解除 _isFetching 與 UI 鎖定、隱藏 loading，並呼叫
                  update_history_to_tws(self.history_datas_of_groups.loc[:, self._pending_column]) 更新畫面，
                  最後將 _pending_column 設回 None。

        回傳：
            None（透過副作用更新：self._history_results、self.history_datas_of_groups、UI 控制項與 TreeWidget 顯示）
        """

        if isinstance(result, Exception):
            QtWidgets.QMessageBox.critical(
                self,
                "歷史負載查詢錯誤",
                f"標籤 {tags} 查詢失敗：{result}"
            )
            return
        # 結果正常，存起來
        key = tuple(tags) # 將接收到的tags 強制轉成str 型別指定給key, 以利後續的issubset 的比對
        self._history_results[key] = result

        # 等到兩組都拿到，才做後續處理
        needed = {tuple(self.thread1.key), tuple(self.thread2.key)}
        if needed.issubset(self._history_results):
            # -------- 計算特定週期，各設備群組(分類)的平均值 -----------
            df1 = self._history_results[tuple(self.thread1.key)]

            mask = ~pd.isnull(self.tag_list.loc[:, 'tag_name2'])  # 作為用來篩選出tag中含有有kwh11 的布林索引器
            groups_demand = self.tag_list.loc[mask, 'tag_name2':'Group2']
            groups_demand.index = self.tag_list.loc[mask, 'name']
            df1.columns = groups_demand.index
            df1 = df1.T  # 將query_result 轉置 shape:(96,178) -> (178,96)
            df1.reset_index(inplace=True, drop=True)  # 重置及捨棄原本的 index
            df1.index = groups_demand.index  # 將index 更新為各迴路或gas 的名稱 (套用groups_demands.index 即可)
            time_list = [t.strftime('%H:%M') for t in pd.date_range('00:00', '23:45', freq='15min')]
            df1.columns = time_list  # 用週期的起始時間，作為各column 的名稱
            df1.loc[:, '00:00':'23:45'] = df1.loc[:, '00:00':'23:45'] * 4  # kwh -> MW/15 min
            groups_demand = pd.concat([groups_demand, df1], axis=1, copy=False)

            wx_list = list()  # 暫存各wx的計算結果用
            for _ in time_list:
                # 利用 group by 的功能，依Group1(單位)、Group2(負載類型)進行分組，將分組結果套入sum()的方法
                wx_grouped = groups_demand.groupby(['Group1', 'Group2'])[_].sum()
                c = wx_grouped.loc['W2':'WA', 'B']
                c.name = _
                c.index = c.index.get_level_values(0)  # 重新將index 設置為原multiIndex 的第一層index 內容
                wx_list.append(c)
            wx = pd.DataFrame([wx_list[_] for _ in range(96)])
            # 將wx 計算結果轉置，並along index 合併於groups_demand 下方, 並將結果存在class 變數中
            self.history_datas_of_groups = pd.concat([groups_demand, wx.T], axis=0)

            # -------- 分析特定週期的 HSM生產時生 -----------
            df2 = self._history_results[tuple(self.thread2.key)]
            # 將資料分類
            # 取出 9h140~9h280、9h180~9kb33 的欄位名稱list
            cols = (list(df2.loc[:, 'W511_HSM/33KV/9H_140/P':'W511_HSM/33KV/9H_280/P'].columns) +
                    list(df2.loc[:, 'W511_HSM/33KV/9H_180/P':'W511_HSM/11.5KV/9KB1_2_33/P'].columns))

            # original_date = pd.DataFrame(df[cols].sum(axis=1),columns=['Main_group'])
            original_date = df2[cols].sum(axis=1)
            filter_date = df2.loc[:, 'W511_HSM/33KV/9H_160/P':'W511_HSM/33KV/9H_170/P'].sum(axis=1)

            # 將所有的資料透過迴圈，15分鍾為一組，透過函式分析 HSM 產線特性，並將結果先以字典的方式儲存，最後再轉成dataframe 格式
            results = {}
            for (t1, win1), (t2, win2) in zip(original_date.resample('15T'), filter_date.resample('15T')):
                assert t1 == t2, f"時間不一致！ HSM 軋延機群={t1}, 要濾掉訊號={t2}"
                results[t1] = analyze_production_avg_cycle(win1, threshold=10, smooth_window=8, prominence=1,
                                                           power_filter=win2, plot=False)

            df_res = pd.DataFrame.from_dict(results, orient='index')

            # 解開重復觸發查詢的 Guard flag 及會觸發查詢的控制項、並隱藏loading overlay
            self._isFetching = False
            self.checkBox_2.setEnabled(True)
            self.dateEdit_3.setEnabled(True)
            self.horizontalScrollBar.setEnabled(True)
            self.loader.hide()

            # 整合完 self.history_datas_of_group 之後，呼叫更新畫面
            self.update_history_to_tws(self.history_datas_of_groups.loc[:, self._pending_column])
            # 清除 pending，避免重複
            self._pending_column = None

    @log_exceptions()
    def history_demand_of_groups(self, st, et):
        """
            查詢特定週期，各設備群組(分類)的平均值
        :param
            st: 查詢的起始時間點
            et: 查詢的最終時間點
        :return:
        """
        if self._isFetching:    # 防止重複觸發查詢的Guard flag
            return
        self._isFetching = True


        # ---------- 準備兩組 tags 清單 ------------
        # ---用來查各種歷史需量值的tags
        mask = ~pd.isnull(self.tag_list.loc[:, 'tag_name2'])  # 作為用來篩選出tag中含有有kwh11 的布林索引器
        groups_demand = self.tag_list.loc[mask, 'tag_name2':'Group2']
        groups_demand.index = self.tag_list.loc[mask, 'name']
        production_line_tags = groups_demand.loc[:, 'tag_name2'].dropna().tolist()  # 把DataFrame 中標籤名為tag_name2 的值，轉成list輸出

        # 用來查詢 HSM 歷史 p值的 tags
        tag_reference = self.tag_list.set_index('name').copy()
        hsm_tags = tag_reference.loc['9H140':'9KB33', 'tag_name'].tolist()

        # 每次查詢前，讓 Overlay 顯示
        # 同時更新 overlay 尺寸，以為剛好主視窗被 resize
        self.loader.setGeometry(self.rect())
        self.loader.show()

        # 先清空先前暫存的，避免影響判斷是否兩個thread 查詢是否完成
        self._history_results.clear()

        # 建立並啟動兩支執行緒
        self.thread1 = PiReader(self.pi_client, key='all_product_line', parent=self)
        self.thread2 = PiReader(self.pi_client, key='hsm', parent=self)

        # 分別呼叫兩個類別實例的 set_query_params() 傳遞參數
        self.thread1.set_query_params(st=st, et=et, tags=production_line_tags)
        self.thread2.set_query_params(st=st, et=et, tags=hsm_tags, summary='AVERAGE',
                                      interval='5s', fillna_method='ffill')

        # 將兩支執行緒都 connect 到同一個槽函式
        self.thread1.data_ready.connect(self.on_data_ready)
        self.thread2.data_ready.connect(self.on_data_ready)

        # 查詢前，把所有會觸發查詢的輸入控制項 disable
        self.checkBox_2.setEnabled(False)
        self.dateEdit_3.setEnabled(False)
        self.horizontalScrollBar.setEnabled(False)

        # 開始執行
        self.thread1.start()
        self.thread2.start()

    def analyze_hsm(self):
        """ 試調分析 HSM 用電資訊 """
        # -- 設定區 --
        interval = self.spinBox_6.value()
        tag_reference = self.tag_list.set_index('name').copy()
        start = pd.Timestamp(self.dateTimeEdit_5.dateTime().toString())
        end = pd.Timestamp(self.dateTimeEdit_5.dateTime().toString()) + pd.offsets.Minute(self.spinBox_5.value())

        # 從PI 系統抓資料
        if self.radioButton_5.isChecked():     # --- 用 kwh 反推 ---
            tags = tag_reference.loc['9H140':'9KB33', 'tag_name2'].tolist()
            df = pi_client.query(start, end, tags, 'RANGE', f'{interval}s', 'ffill')
            df = df * 3600 / interval

            # 針對9h160、9h170 的 KWH值，從原始HSM 設備群中挑出來，提高分析生產速率和數量的準確性。
            filter_date = df.loc[:,'W511_HSM/33KV/9H_160/kwh11':'W511_HSM/33KV/9H_170/kwh11'].sum(axis=1)
        else:
            tags = tag_reference.loc['9H140':'9KB33', 'tag_name'].tolist()
            df = pi_client.query(start, end, tags, 'AVERAGE', f'{interval}s', 'ffill')

            # 針對9h160、9h170 的 P值，從原始HSM 設備群中挑出來，提高分析生產速率和數量的準確性。
            filter_date = df.loc[:,'W511_HSM/33KV/9H_160/P':'W511_HSM/33KV/9H_170/P'].sum(axis=1)

        original_date = df.sum(axis=1)

        # 呼叫 data_analysis 的 analyze_production_avg_cycle
        res3 = analyze_production_avg_cycle(original_date, threshold=self.spinBox_3.value(),
                                            smooth_window=int(40/interval), prominence=self.spinBox_4.value(),
                                            power_filter=filter_date, plot=True)

    def on_show_trend(self):
        """趨勢圖測試區"""
        interval = self.spinBox_6.value()
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

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """
            ❶ 覆寫 closeEvent 就能攔截使用者關窗的動作
            在關閉視窗時，優牙中斷這條背景執行緒
        """
        for attr in ("scheduler_thread", "dashboard_thread"):
            thread = getattr(self, attr, None)
            if thread is not None:
                thread.requestInterruption()
                thread.quit()
                thread.wait()
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
        初始化 tw1、tw2、tw3 以及 tw1_2、tw2_2、tw3_2 的樹狀表格內容格式。

        - 遍歷每個 topLevelItem，呼叫 init_tree_item() 完成對齊與配色。
        - tw1/tw1_2 的頂層即時量使用獨立顏色，tw2/tw2_2、tw3/tw3_2 則沿用一般規則。
        - 新增對 *_2 TreeWidget 的支援，僅套用共有欄位（0、1）的樣式。

        Note:
            此方法集中處理所有 TreeWidget 的初始化，避免後續維護時需要個別設定。
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

        # (2025/09/07): 初始化tw1_2, tw2_2, tw3_2, 但僅影響共有欄位(0、1)
        for tree in [getattr(self, "tw1_2", None), getattr(self, "tw2_2", None), getattr(self, "tw3_2", None)]:
            if tree is None:
                continue
            for i in range(tree.topLevelItemCount()):
                # tw1_2 延續 tw1 的頂層顏色，其餘同tw2/3
                self.init_tree_item(
                    tree.topLevelItem(i),
                    level=0,
                    level0_color=(brush_top if tree is getattr(self, "tw1_2", None) else None),
                    level_sub_color=brush_sub
                )

    def init_tree_item(self, item, level, level0_color=None, level_sub_color=None, ):
        """
        建立並初始化一個 QTreeWidgetItem，並依照指定的 widget 與 column 規則套用字型與對齊方式。

        規則：
        - 所有欄位都會套用預設字型大小。
        - tw1 ~ tw3：
            - column 1、2 → 文字靠右、垂直置中。
        - tw*_2：
            - column 1 → 文字靠右、垂直置中。
            - column 2 → 文字置中。
        - 其它 widget → 全部置中。
        """
        tw = item.treeWidget()
        name = tw.objectName() or ""  # 以 objectName 辨識 tw1_2 / tw2_2 / tw3_2
        is_secondary = name in ("tw1_2", "tw2_2", "tw3_2")

        # 設定欄位對齊方式
        align0 = QtCore.Qt.AlignmentFlag.AlignCenter if level != 1 else QtCore.Qt.AlignmentFlag.AlignLeft
        align1 = QtCore.Qt.AlignmentFlag.AlignRight
        align_2 = (
            QtCore.Qt.AlignmentFlag.AlignCenter if is_secondary
            else QtCore.Qt.AlignmentFlag.AlignRight
        )

        max_cols = tw.columnCount()
        if max_cols > 0:
            item.setTextAlignment(0, align0)
        if max_cols > 1:
            item.setTextAlignment(1, align1)
        if max_cols > 2:
            item.setTextAlignment(2, align_2)

        # 設定顏色
        if level == 0 and level0_color is not None and item.treeWidget().columnCount() > 1:
            item.setForeground(1, level0_color) # 頂層即時量顏色

        elif level >= 2 and level_sub_color is not None and item.treeWidget().columnCount() > 1:
            item.setForeground(1, level_sub_color)  # 內層即時量顏色

        # 遞迴處理子節點
        for i in range(item.childCount()):
            self.init_tree_item(item.child(i), level + 1, level0_color, level_sub_color)

    def beautify_tree_widgets(self):
        """
        統一美化 tw1, tw2, tw3, tw4 與 tw1_2, tw2_2, tw3_2 的表頭樣式與欄位寬度。

        - 一律只針對 QHeaderView 設定樣式，避免與 widget 級別樣式互相覆蓋。
        - 顏色主題：
            * 藍紫：tw1, tw2, tw1_2, tw2_2
            * 綠藍：tw3, tw3_2
        - 欄寬維持你目前的設定：
            * tw1/tw2/tw3: [175, 90, 65]
            * tw4: col0=190, col1=210 並將 section resize 設為 Fixed
            * tw1_2/tw3_2：套用對應 tw1/tw3 的 col0、col1（僅共通欄）
            * tw2_2：col0=130、col1=沿用 tw2 的第二欄寬、col2=270
        - 表頭字級統一（例如 11pt；若你想回到舊字級，改 header_point_sz 即可）。

        注意：
        本函式不處理「即時量(col=1) / 平均值(col=2)」的 item 顏色；請沿用你在 init_tree_item() /
        產樹流程中對每個 QTreeWidgetItem 的 setForeground/setBackground，避免重複設定。
        """

        # ---- 漸層樣式（header 專用）----
        style_blue = (
            "QHeaderView::section { "
            "background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #52e5e7, stop:1 #130cb7); "
            "color: white; font-weight: bold; }"
        )
        style_green = (
            "QHeaderView::section { "
            "background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #0e6499, stop:1 #9fdeab); "
            "color: white; font-weight: bold; }"
        )

        header_point_sz = 11  # 與你目前檔案相符的字級；需要微調可改這個數字

        # ---- 依名稱套主題並統一字級 ----
        for name in ("tw1", "tw2", "tw1_2", "tw2_2"):
            w = getattr(self, name, None)
            if not w:
                continue
            h = w.header()
            h.setStyleSheet(style_blue)
            hf = h.font()
            hf.setPointSize(header_point_sz)
            h.setFont(hf)

        for name in ("tw3", "tw3_2"):
            w = getattr(self, name, None)
            if not w:
                continue
            h = w.header()
            h.setStyleSheet(style_green)
            hf = h.font()
            hf.setPointSize(header_point_sz)
            h.setFont(hf)

        # ---- 欄寬 ----
        column_widths = {"tw1": [175, 90, 65], "tw2": [175, 90, 65], "tw3": [175, 90, 65]}

        for name in ("tw1", "tw2", "tw3"):
            w = getattr(self, name, None)
            if not w:
                continue
            w.setColumnWidth(0, column_widths[name][0])
            w.setColumnWidth(1, column_widths[name][1])
            w.setColumnWidth(2, column_widths[name][2])

        # 確保 tw4 有 objectName（非必要，但有助於未來 CSS 定位）
        if not self.tw4.objectName():
            self.tw4.setObjectName("tw4")

        # 將 tw4 表頭改成橘粉漸層（與先前一致）
        self.tw4.header().setStyleSheet(
            "QHeaderView::section { "
            "background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #fad7a1, stop:1 #e96d71); "
            "color: white; font-weight: bold; }"
        )
        hf = self.tw4.header().font()
        hf.setPointSize(header_point_sz)
        self.tw4.header().setFont(hf)

        # tw4 欄寬與固定
        if getattr(self, "tw4", None):
            self.tw4.setColumnWidth(0, 190)
            self.tw4.setColumnWidth(1, 210)  # 你原本也有 200 或 210，統一成 210 可視需求調
            self.tw4.header().setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.Fixed)
            self.tw4.header().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Fixed)

        # tw1_2 / tw3_2：對應 tw1 / tw3 的前兩欄欄寬（僅共通欄）
        if getattr(self, "tw1_2", None):
            self.tw1_2.setColumnWidth(0, column_widths["tw1"][0])
            self.tw1_2.setColumnWidth(1, column_widths["tw1"][1])

        if getattr(self, "tw3_2", None):
            self.tw3_2.setColumnWidth(0, column_widths["tw3"][0])
            self.tw3_2.setColumnWidth(1, column_widths["tw3"][1])

        # tw2_2：你先前指定的 0/1/2 欄寬（第 1 欄沿用 tw2 的寬度）
        if getattr(self, "tw2_2", None):
            self.tw2_2.setColumnWidth(0, 130)
            self.tw2_2.setColumnWidth(1, column_widths["tw2"][1])
            if self.tw2_2.columnCount() > 2:
                self.tw2_2.setColumnWidth(2, 270)

        # **確保 tw4.clear() 不影響 header**
        self.tw4.setHeaderLabels(["製程種類 & 排程時間", "狀態"])

        # tw1/tw2/tw3：col=1(即時量) + col=2(平均值)
        for widget in [self.tw1, self.tw2, self.tw3]:
            if widget is None:
                continue
            it = QtWidgets.QTreeWidgetItemIterator(widget)
            while it.value():
                item = it.value()
                # col=1 即時量
                if widget.columnCount() > 1:
                    item.setFont(1, QtGui.QFont("微軟正黑體", 12))
                    item.setBackground(1, QtGui.QBrush(QtGui.QColor(self.real_time_back)))
                    item.setForeground(1, QtGui.QBrush(QtGui.QColor(self.real_time_text)))
                    item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
                # col=2 平均值
                if widget.columnCount() > 2:
                    item.setFont(2, QtGui.QFont("微軟正黑體", 12, QtGui.QFont.Weight.Bold))
                    item.setBackground(2, QtGui.QBrush(QtGui.QColor("#D6EAF8")))
                    item.setForeground(2, QtGui.QBrush(QtGui.QColor("#154360")))
                    item.setTextAlignment(2, QtCore.Qt.AlignmentFlag.AlignRight)
                it += 1

        # tw*_2：僅 col=1（即時量）配色；col=2 留給你的排程/字級 9 pt 流程處理
        for widget in [getattr(self, "tw1_2", None), getattr(self, "tw2_2", None), getattr(self, "tw3_2", None)]:
            if widget is None or widget.columnCount() <= 1:
                continue
            it = QtWidgets.QTreeWidgetItemIterator(widget)
            while it.value():
                item = it.value()
                item.setFont(1, QtGui.QFont("微軟正黑體", 12))
                item.setBackground(1, QtGui.QBrush(QtGui.QColor(self.real_time_back)))
                item.setForeground(1, QtGui.QBrush(QtGui.QColor(self.real_time_text)))
                item.setTextAlignment(1, QtCore.Qt.AlignmentFlag.AlignRight)
                it += 1

        # **針對 tw1 & tw3 (TGs, TG1~TG4) 的即時量，讓它能隨展開事件改變顏色**
        self.tw1.itemExpanded.connect(self.tw1_expanded_event)
        self.tw1.itemCollapsed.connect(self.tw1_expanded_event)
        self.tw3.itemExpanded.connect(self.tw3_expanded_event)
        self.tw3.itemCollapsed.connect(self.tw3_expanded_event)

    def beautify_table_widgets(self):
        """ 使用 setStyleSheet() 統一美化 tableWidget_3 的表頭 """

        # **透過 setStyleSheet() 設定表頭統一風格**
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
        self.tableWidget_3.setItem(3, 0, make_item('台電供電量\n(需量)', bold=False, font_size=8))

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
        tw3_base_width = (self.tw3.columnWidth(0) + self.tw3.columnWidth(1) +20)
        base_width = self.tableWidget_3.columnWidth(0) + self.tableWidget_3.columnWidth(1)

        if self.checkBox_2.isChecked():     # 顯示歷史平均值
            # -----------調出當天的各週期平均 (透過dateEdit_3 變更所發出的信號，再由對應的函式執行 -----------
            st = pd.Timestamp.today().date()
            self.dateEdit_3.setDate(QtCore.QDate(st.year, st.month, st.day))

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

        name_list = self.tag_list['tag_name'].dropna().tolist()     # 1
        try:
            current = pi_client.current_values(name_list)           # 2
            # 如果之前有錯誤訊息，先清掉
            self.statusBar().clearMessage()
        except Exception as e:
            logger.error(f"[dashboard_value] PI 連線失敗:{e}")
            # 在 statusBar 顯示一條不會自動消失的警告
            self.statusBar().showMessage("⚠⚠ 無法連線到 PI Server，請檢查網路或憑證 ⚠⚠", 0)
            return # 直接結束，避免後面用到 current 而再度崩潰！

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
        self.realtime_update_to_tws(c_values)
        self.label_23.setText(str(f'%s MW' %(self.predict_demand())))

        # 更新hsm 目前速率及每卷需量
        self.real_time_hsm_cycle()
        return c_values

    def predict_demand(self):
        """
        預估本 15 分鐘週期完成時的「最終需量」（即將來到的區段平均功率）。

        概念
        ----
        - 以目前週期已經累積的需量，加上「近 300 秒的平均需量」外推至本週期剩餘秒數的貢獻量：
          預測 = 目前累積 + (最近300秒平均 / 300) * 剩餘秒數

        時間處理
        --------
        - 週期起訖：st = now().floor('15T')、et = st + 15 分鐘。
        - 「現在」一律取整秒 pd.Timestamp.now().floor('s')，避免小數秒造成邏輯偏差。
        - 近 300 秒時間窗：[now-300s, now)。

        模式
        ----
        - kWh 模式（radioButton_5 勾選）：
            * 週期內累積量：直接查詢 kWh tag（1510/1520），相加後乘以 4 得到「目前週期至今」的需量累積。
            * 近窗平均：對同兩 tag 在最近 300 秒區間相加乘 4，取其平均後按「剩餘秒數」線性外推。
        - P 模式（radioButton_5 未勾選）：
            * 以 summary="AVERAGE"、秒級 interval 讀取功率，先 clip(lower=0)，對未來時間造成的 NaN 以 0 補，
              再 resample('15T').mean() 將目前週期的均值視為「已累積」，並用近 300 秒平均推估剩餘貢獻。

        回傳
        ----
        float
            本 15 分鐘區段的預測需量（四捨五入到小數點後 2 位）。

        注意
        ----
        - 本方法假設「近 300 秒的平均」可代表剩餘時間的用電行為（線性外推）。
        - P 模式會把負功率視為 0；對未來造成的 NaN 先以 0 補齊再進行平均。
        """
        time_window = 300                   # 滾動平均值的時間窗長度
        st = pd.Timestamp.now().floor('15T')    # 目前週期的起始時間
        et = st + pd.offsets.Minute(15)         # 目前週期的結束時間

        # now() 必須把當前時間「往下取整的整秒」，避免後續計算出問題。
        back_300s_from_now = pd.Timestamp.now().floor('s') - pd.offsets.Second(time_window)
        diff_between_now_and_et = (et - pd.Timestamp.now().floor('s')).total_seconds()  # 此週期剩餘時間

        # 根據radioButton_5，判斷用kwh 或p 計算需量。
        if self.radioButton_5.isChecked():
            tags=('W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11')
            # 查詢目前週期的累計需量值
            query_result = pi_client.query(st=st, et=et, tags=tags)
            current_accumulation = query_result.sum(axis=1) * 4

            # 查近time_window秒的平均需量，並計算出剩餘時間可能會增加的需量累計值
            result = pi_client.query(st=back_300s_from_now, et=back_300s_from_now + pd.offsets.Second(time_window),
                                     tags=tags)
            result = result.sum(axis=1) * 4   # MWH * 60min / 15min = MW/15min
            weight = 1 / time_window * diff_between_now_and_et
            predict = result * weight
            demand = round((current_accumulation[0] + predict[0]), 2)
        else:
            tags=('W511_MS1/161KV/1510/P', 'W511_MS1/161KV/1520/P')
            query_result = pi_client.query(st=st, et=et, tags=tags, summary="AVERAGE", interval= f'3s')
            query_result = query_result.clip(lower=0)
            # 考慮部份時間範圍屬於未來時間而有nan值，所以將必須先將nan值轉為0。
            query_result = query_result.fillna(0).resample('15T').mean()
            current_accumulation = query_result.sum(axis=1)

            # 查近time_window秒的平均需量，並計算出剩餘時間可能會增加的需量累計值
            result = pi_client.query(st=back_300s_from_now,
                                     et=back_300s_from_now + pd.offsets.Second(time_window),
                                     tags=tags, summary="AVERAGE", interval=f'3s')
            result = result.clip(lower=0).mean()        # (MW/time_window)
            result = result.sum() * time_window / 900   # MW/time_window * time_window / 15min = MW/15min
            weight = 1 / time_window * diff_between_now_and_et
            predict = result * weight
            demand = round((current_accumulation[0] + predict),2)
        return demand

    def date_edit3_user_change(self, new_date:QtCore.QDate):
        if self.dateEdit_3.date() >= pd.Timestamp.today().date():
            # ----選定到 "未來" 或當天的日期時，查詢今天的各週期資料，並顯示今天的最後一個結束週期的資料----
            sd = pd.Timestamp(pd.Timestamp.now().date())
            ed = sd + pd.offsets.Day(1)
            self.history_demand_of_groups(st=sd, et=ed)

            # 將et 設定在最接近目前時間點之前的最後15分鐘結束點, 並將 scrollerBar 調整至相對應的值
            et = pd.Timestamp.now().floor('15T')
            st = et - pd.offsets.Minute(15)
            self.label_16.setText(st.strftime('%H:%M'))
            self.label_17.setText(et.strftime('%H:%M'))

            # 設定水平scrollBar 時，要先block signal, 避免執行多次查詢及更新資料
            self.horizontalScrollBar.blockSignals(True)
            self.horizontalScrollBar.setValue((et - pd.Timestamp.now().normalize()) // pd.Timedelta('15T') - 1)
            self.horizontalScrollBar.blockSignals(False)

            # 先記錄要更新的 column，作為後續呼叫更新畫面時的key
            self._pending_column = st.strftime('%H:%M')

        elif self.dateEdit_3.date() < pd.Timestamp.today().date():
            #  ---- 查詢歷史資料 ----
            sd = pd.Timestamp(self.dateEdit_3.date().toString())
            ed = sd + pd.offsets.Day(1)
            self.history_demand_of_groups(st=sd, et=ed)

            # ------ 日期為過去時，則顯示第一個週期的資料 ------
            self.label_16.setText('00:00')
            self.label_17.setText('00:15')

            # 設定水平scrollBar 時，要先block signal, 避免執行多次查詢及更新資料
            self.horizontalScrollBar.blockSignals(True)
            self.horizontalScrollBar.setValue(0)
            self.horizontalScrollBar.blockSignals(False)

            # 先記錄要更新的 column，作為後續呼叫更新畫面時的key
            self._pending_column = '00:00'

    def scroller_changed_event(self):
        """
        scrollbar 數值變更後，判斷是否屬於未來時間，並依不同狀況執行相對應的區間、紀錄顯示
        """
        now = pd.Timestamp.now()
        current_date_widget3 = pd.Timestamp(self.dateEdit_3.date().toString())
        # 依據水平捲軸的值計算所選的區間
        st = current_date_widget3 + pd.offsets.Minute(15) * self.horizontalScrollBar.value()
        et = st + pd.offsets.Minute(15)

        # 如果查詢日期為今天，檢查是否需要刷新歷史資料
        if current_date_widget3.normalize() == now.normalize():
            # 過濾出符合時間格式的欄位，取得目前已查詢的最晚時間欄位

            time_columns = [col for col in self.history_datas_of_groups.columns if re.match(r'^\d{2}:\d{2}$', str(col))]
            # 過濾掉全部為 NaN 的欄位
            valid_time_columns = [t for t in time_columns if self.history_datas_of_groups[t].dropna().size > 5]
            if valid_time_columns:
                last_completed_time_str = max(valid_time_columns,
                                              key=lambda t: pd.Timestamp(f"{current_date_widget3.date()} {t}"))
                max_time = pd.Timestamp(f"{current_date_widget3.date()} {last_completed_time_str}")
                # 如果指定的時間區域，已超過現有資料的時間範圍（表示有新完成的區間）
                if et > max_time:
                    # 重新查詢整天的歷史資料更新到最新狀態
                    self.history_demand_of_groups(st=current_date_widget3, et=current_date_widget3
                                                                              + pd.offsets.Day(1))

        # 如果選取的區間 et 超過目前時間，則調整至最後完成的區間
        if et > now:
            et = now.floor('15T')
            # 重新計算對應的水平捲軸值
            self.horizontalScrollBar.setValue(((et - current_date_widget3) // pd.Timedelta('15T')) - 1)
            st = et - pd.offsets.Minute(15)

        self.label_16.setText(st.strftime('%H:%M'))
        self.label_17.setText(et.strftime('%H:%M'))

        # 先記錄要更新的 column，作為後續呼叫更新畫面時的key
        self._pending_column = st.strftime('%H:%M')
        # 整合完 self.history_datas_of_group 之後，呼叫更新畫面
        self.update_history_to_tws(self.history_datas_of_groups.loc[:, self._pending_column])

    def update_history_to_tws(self, current_p):
        """
        暫時用來將各群組的歷史平均量顯顯示在 各tree widget 的3rd column
        :param current_p:
        :return:
        """
        # tw1（歷史平均欄 col=2)
        w2_total = current_p['2H180':'2KB41'].sum() + current_p['W2']
        w3_total = current_p['AJ320':'5KB28'].sum() + current_p['W3']
        w41_utility = current_p['W4']
        w42_utility = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_utility = w41_utility + w42_utility
        w41_main = current_p['AJ130':'AJ170'].sum()
        w4_total = w41_main + w4_utility
        w5_subtotal = current_p['3KA14':'2KB29'].sum() + current_p['W5']
        self._set(self.tw1, 2, (0,), w2_total, avg=True)
        self._set(self.tw1, 2, (0, 0,), current_p['2H180':'1H350'].sum(), avg=True)
        self._set(self.tw1, 2, (0, 0, 0,), current_p['2H180'], avg=True)
        self._set(self.tw1, 2, (0, 0, 1,), current_p['2H280'], avg=True)
        self._set(self.tw1, 2, (0, 0, 2,), current_p['1H350'], avg=True)
        self._set(self.tw1, 2, (0, 1,), current_p['4KA19'], avg=True)
        self._set(self.tw1, 2, (0, 2,), current_p['4KB19':'4KB29'].sum(), avg=True)
        self._set(self.tw1, 2, (0, 2, 0,), current_p['4KB19'], avg=True)
        self._set(self.tw1, 2, (0, 2, 1,), current_p['4KB29'], avg=True)
        self._set(self.tw1, 2, (0, 3,), current_p['2KA41':'2KB41'].sum(), avg=True)
        self._set(self.tw1, 2, (0, 3, 0,), current_p['2KA41'], avg=True)
        self._set(self.tw1, 2, (0, 3, 1,), current_p['2KB41'], avg=True)
        self._set(self.tw1, 2, (0, 4,), current_p['W2'], avg=True)
        self._set(self.tw1, 2, (1,), w3_total, avg=True)
        self._set(self.tw1, 2, (1, 0,), current_p['AJ320'], avg=True)
        self._set(self.tw1, 2, (1, 1,), current_p['5KA18':'5KB28'].sum(), avg=True)
        self._set(self.tw1, 2, (1, 1, 0,), current_p['5KA18'], avg=True)
        self._set(self.tw1, 2, (1, 1, 1,), current_p['5KA28'], avg=True)
        self._set(self.tw1, 2, (1, 1, 2,), current_p['5KB18'], avg=True)
        self._set(self.tw1, 2, (1, 1, 3,), current_p['5KB28'], avg=True)
        self._set(self.tw1, 2, (1, 2,), current_p['W3'], avg=True)
        self._set(self.tw1, 2, (2,), w4_total, pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw1, 2, (2, 0,), w41_main, pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw1, 2, (2, 1,), w4_utility, pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw1, 2, (3,), w5_subtotal, avg=True)
        self._set(self.tw1, 2, (3,0,), current_p['3KA14':'3KA15'].sum(), avg=True)
        self._set(self.tw1, 2, (3, 0, 0,), current_p['3KA14'], avg=True)
        self._set(self.tw1, 2, (3, 0, 1,), current_p['3KA15'], avg=True)
        self._set(self.tw1, 2, (3, 1,), current_p['3KA24':'3KA25'].sum(), avg=True)
        self._set(self.tw1, 2, (3, 1, 0,), current_p['3KA24'], avg=True)
        self._set(self.tw1, 2, (3, 1, 1,), current_p['3KA25'], avg=True)
        self._set(self.tw1, 2, (3, 2,), current_p['3KB12':'3KB28'].sum(), avg=True)
        self._set(self.tw1, 2, (3, 2, 0,), current_p['3KB12'], avg=True)
        self._set(self.tw1, 2, (3, 2, 1,), current_p['3KB22'], avg=True)
        self._set(self.tw1, 2, (3, 2, 2,), current_p['3KB28'], avg=True)
        self._set(self.tw1, 2, (3, 3,), current_p['3KA16':'3KB27'].sum(), avg=True)
        self._set(self.tw1, 2, (3, 3, 0,), current_p['3KA16'], avg=True)
        self._set(self.tw1, 2, (3, 3, 1,), current_p['3KA26'], avg=True)
        self._set(self.tw1, 2, (3, 3, 2,), current_p['3KA17'], avg=True)
        self._set(self.tw1, 2, (3, 3, 3,), current_p['3KA27'], avg=True)
        self._set(self.tw1, 2, (3, 3, 4,), current_p['3KB16'], avg=True)
        self._set(self.tw1, 2, (3, 3, 5,), current_p['3KB26'], avg=True)
        self._set(self.tw1, 2, (3, 3, 6,), current_p['3KB17'], avg=True)
        self._set(self.tw1, 2, (3, 3, 7,), current_p['3KB27'], avg=True)
        self._set(self.tw1, 2, (3, 4,), current_p['2KA19':'2KB29'].sum(), avg=True)
        self._set(self.tw1, 2, (3, 4, 0,), current_p['2KA19'], avg=True)
        self._set(self.tw1, 2, (3, 4, 1,), current_p['2KA29'], avg=True)
        self._set(self.tw1, 2, (3, 4, 2,), current_p['2KB19'], avg=True)
        self._set(self.tw1, 2, (3, 4, 3,), current_p['2KB29'], avg=True)
        self._set(self.tw1, 2, (3, 5,), current_p['W5'], avg=True)
        self._set(self.tw1, 2, (4,), current_p['WA'], avg=True)

        # tw2（歷史平均欄 col=2)
        self._set(self.tw2, 2, (0,), current_p['9H140':'9KB33'].sum(), pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw2, 2, (1,), current_p['AH120'], pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw2, 2, (2,), current_p['AH190'], pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw2, 2, (3,), current_p['AH130'], pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw2, 2, (4,), current_p['1H450'], pre_kwargs=dict(b=0), avg=True)
        self._set(self.tw2, 2, (5,), current_p['1H360'], pre_kwargs=dict(b=0), avg=True)

        # tw3（歷史平均欄 col=2)
        self._set(self.tw3, 2, (0, ), current_p['2H120':'1H420'].sum(), avg=True)
        self._set(self.tw3, 2, (0, 0,), current_p['2H120':'2H220'].sum(), avg=True)
        self._set(self.tw3, 2, (0, 1,), current_p['5H120':'5H220'].sum(), avg=True)
        self._set(self.tw3, 2, (0, 2,), current_p['1H120':'1H220'].sum(), avg=True)
        self._set(self.tw3, 2, (0, 3,), current_p['1H320':'1H420'].sum(), avg=True)
        self._set(self.tw3, 2, (1, ), current_p['4KA18':'5KB19'].sum(), avg=True)
        self._set(self.tw3, 2, (1, 0,), current_p['4KA18'].sum(), avg=True)
        self._set(self.tw3, 2, (1, 1,), current_p['5KB19'].sum(), avg=True)
        self._set(self.tw3, 2, (2, ), current_p['4H120':'4H220'].sum(), avg=True)
        self._set(self.tw3, 2, (2, 0,), current_p['4H120'].sum(), avg=True)
        self._set(self.tw3, 2, (2, 1,), current_p['4H220'].sum(), avg=True)

        sun_power = current_p['9KB25-4_2':'3KA12-1_2'].sum()
        tai_power_demand = current_p['feeder 1510':'feeder 1520'].sum()
        reversed_power = current_p['feeder 1510_s':'feeder 1520_s'].sum()
        full_load = tai_power_demand - reversed_power + current_p['2H120':'5KB19'].sum() - sun_power


        self.update_table_item(0, 2, self.pre_check2(full_load), self.average_back, self.average_text, bold=True)
        self.update_table_item(1, 2, self.pre_check2(current_p['2H120':'5KB19'].sum()), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(2, 2, self.pre_check2(sun_power, b=5), self.average_back,
                               self.average_text, bold=True)
        self.update_table_item(3, 2, str(format(round(tai_power_demand,2))), self.average_back,
                               self.average_text, bold=True)

        # error_value & w5_total correction
        dynamic_load = current_p['AH120':'9KB33'].sum()
        error_value = (full_load -w2_total - w3_total -w4_total - w5_subtotal - dynamic_load - current_p['WA'])
        self.tw1.topLevelItem(3).child(6).setText(2, str(format(round(error_value, 2), '.2f')))
        w5_total = w5_subtotal + error_value
        self.tw1.topLevelItem(3).setText(2, self.pre_check2(w5_total))

    def realtime_update_to_tws(self, current_p):
        """
        將電力系統的即時資訊，更新至對應的樹狀結構(tree widget)、表格結構(table widget)
        :param current_p: 即時用電量。pd.Series
        :return:
        """

        # tw1（即時欄 col=1）
        w2_total = current_p['2H180':'2KB41'].sum() + current_p['W2']
        w3_total = current_p['AJ320':'5KB28'].sum() + current_p['W3']
        w41_utility = current_p['W4']
        w42_utility = current_p['9H110':'9H210'].sum() - current_p['9H140':'9KB33'].sum()
        w4_utility = w41_utility + w42_utility
        w41_main = current_p['AJ130':'AJ170'].sum()
        w4_total = w41_main + w4_utility
        w5_subtotal = current_p['3KA14':'2KB29'].sum() + current_p['W5']

        self._set(self.tw1, 1, (0,), w2_total)
        self._set(self.tw1, 1, (0, 0,), current_p['2H180':'1H350'].sum())
        self._set(self.tw1, 1, (0, 0, 0,), current_p['2H180'])
        self._set(self.tw1, 1, (0, 0, 1,), current_p['2H280'])
        self._set(self.tw1, 1, (0, 0, 2,), current_p['1H350'])
        self._set(self.tw1, 1, (0, 1,), current_p['4KA19'])
        self._set(self.tw1, 1, (0, 2,), current_p['4KB19':'4KB29'].sum())
        self._set(self.tw1, 1, (0, 2, 0,), current_p['4KB19'])
        self._set(self.tw1, 1, (0, 2, 1,), current_p['4KB29'])
        self._set(self.tw1, 1, (0, 3,), current_p['2KA41':'2KB41'].sum())
        self._set(self.tw1, 1, (0, 3, 0,), current_p['2KA41'])
        self._set(self.tw1, 1, (0, 3, 1,), current_p['2KB41'])
        self._set(self.tw1, 1, (0, 4,), current_p['W2'])
        self._set(self.tw1, 1, (1,), w3_total)
        self._set(self.tw1, 1, (1, 0,), current_p['AJ320'])
        self._set(self.tw1, 1, (1, 1,), current_p['5KA18':'5KB28'].sum())
        self._set(self.tw1, 1, (1, 1, 0,), current_p['5KA18'])
        self._set(self.tw1, 1, (1, 1, 1,), current_p['5KA28'])
        self._set(self.tw1, 1, (1, 1, 2,), current_p['5KB18'])
        self._set(self.tw1, 1, (1, 1, 3,), current_p['5KB28'])
        self._set(self.tw1, 1, (1, 2,), current_p['W3'])
        self._set(self.tw1, 1, (2,), w4_total)
        self._set(self.tw1, 1, (2, 0,), w41_main, pre_kwargs=dict(b=4))
        self._set(self.tw1, 1, (2, 1,), w4_utility)
        self._set(self.tw1, 1, (3,), w5_subtotal)
        self._set(self.tw1, 1, (3,0,), current_p['3KA14':'3KA15'].sum())
        self._set(self.tw1, 1, (3, 0, 0,), current_p['3KA14'])
        self._set(self.tw1, 1, (3, 0, 1,), current_p['3KA15'])
        self._set(self.tw1, 1, (3, 1,), current_p['3KA24':'3KA25'].sum())
        self._set(self.tw1, 1, (3, 1, 0,), current_p['3KA24'])
        self._set(self.tw1, 1, (3, 1, 1,), current_p['3KA25'])
        self._set(self.tw1, 1, (3, 2,), current_p['3KB12':'3KB28'].sum())
        self._set(self.tw1, 1, (3, 2, 0,), current_p['3KB12'])
        self._set(self.tw1, 1, (3, 2, 1,), current_p['3KB22'])
        self._set(self.tw1, 1, (3, 2, 2,), current_p['3KB28'])
        self._set(self.tw1, 1, (3, 3,), current_p['3KA16':'3KB27'].sum())
        self._set(self.tw1, 1, (3, 3, 0,), current_p['3KA16'])
        self._set(self.tw1, 1, (3, 3, 1,), current_p['3KA26'])
        self._set(self.tw1, 1, (3, 3, 2,), current_p['3KA17'])
        self._set(self.tw1, 1, (3, 3, 3,), current_p['3KA27'])
        self._set(self.tw1, 1, (3, 3, 4,), current_p['3KB16'])
        self._set(self.tw1, 1, (3, 3, 5,), current_p['3KB26'])
        self._set(self.tw1, 1, (3, 3, 6,), current_p['3KB17'])
        self._set(self.tw1, 1, (3, 3, 7,), current_p['3KB27'])
        self._set(self.tw1, 1, (3, 4,), current_p['2KA19':'2KB29'].sum())
        self._set(self.tw1, 1, (3, 4, 0,), current_p['2KA19'])
        self._set(self.tw1, 1, (3, 4, 1,), current_p['2KA29'])
        self._set(self.tw1, 1, (3, 4, 2,), current_p['2KB19'])
        self._set(self.tw1, 1, (3, 4, 3,), current_p['2KB29'])
        self._set(self.tw1, 1, (3, 5,), current_p['W5'])
        self._set(self.tw1, 1, (4,), current_p['WA'])

        # tw2（即時欄 col=1)
        self._set(self.tw2, 1, (0,), current_p['9H140':'9KB33'].sum(), pre_kwargs=dict(b=0))
        self._set(self.tw2, 1, (1,), current_p['AH120'], pre_kwargs=dict(b=0))
        self._set(self.tw2, 1, (2,), current_p['AH190'], pre_kwargs=dict(b=0))
        self._set(self.tw2, 1, (3,), current_p['AH130'], pre_kwargs=dict(b=0))
        self._set(self.tw2, 1, (4,), current_p['1H450'], pre_kwargs=dict(b=0))
        self._set(self.tw2, 1, (5,), current_p['1H360'], pre_kwargs=dict(b=0))

        # tw3（即時欄 col=1)
        ng_to_power = get_ng_generation_cost_v2(self.unit_prices).get("convertible_power")
        #ng_to_power = self.unit_prices.loc['可轉換電力', 'current']

        self._set(self.tw3, 1, (0, ), current_p['2H120':'1H420'].sum())
        self._set(self.tw3, 1, (0, 0,), current_p['2H120':'2H220'].sum())
        self._set(self.tw3, 1, (0, 1,), current_p['5H120':'5H220'].sum())
        self._set(self.tw3, 1, (0, 2,), current_p['1H120':'1H220'].sum())
        self._set(self.tw3, 1, (0, 3,), current_p['1H320':'1H420'].sum())
        self._set(self.tw3, 1, (1, ), current_p['4KA18':'5KB19'].sum())
        self._set(self.tw3, 1, (1, 0,), current_p['4KA18'].sum())
        self._set(self.tw3, 1, (1, 1,), current_p['5KB19'].sum())
        self._set(self.tw3, 1, (2, ), current_p['4H120':'4H220'].sum())
        self._set(self.tw3, 1, (2, 0,), current_p['4H120'].sum())
        self._set(self.tw3, 1, (2, 1,), current_p['4H220'].sum())

        # tw3 的TGs 及其子節點 TG1~TG4 的 NG貢獻電量、使用量，從原本顯示在最後兩個column，改為顯示在3rd 的tip
        ng = pd.Series([current_p['TG1 NG':'TG4 NG'].sum(), current_p['TG1 NG'], current_p['TG2 NG'],
                        current_p['TG3 NG'], current_p['TG4 NG'], ng_to_power])
        self.update_tw3_tips_and_colors(ng)

        # 方式 2：table widget 3 利用 self.update_table_item 函式，在更新內容後，保留原本樣式不變
        full_load = current_p['feeder 1510':'feeder 1520'].sum() + current_p['2H120':'5KB19'].sum() \
                    - current_p['sp_real_time']
        tai_power_demand = str(format(round(current_p['feeder 1510':'feeder 1520'].sum(), 2), '.2f')) + ' MW'

        self.update_table_item(0, 1, self.pre_check(full_load), self.real_time_back, self.real_time_text)
        self.update_table_item(1, 1, self.pre_check(current_p['2H120':'5KB19'].sum()), self.real_time_back, self.real_time_text)  # 即時量
        self.update_table_item(2, 1, self.pre_check(current_p['sp_real_time'], b=5), self.real_time_back, self.real_time_text)
        self.update_table_item(3, 1, tai_power_demand , self.real_time_back, self.real_time_text)

        # error_value & w5_total correction
        dynamic_load = current_p['AH120':'9KB33'].sum()
        error_value = (full_load -w2_total - w3_total -w4_total - w5_subtotal - dynamic_load - current_p['WA'])
        self.tw1.topLevelItem(3).child(6).setText(1, str(format(round(error_value, 2), '.2f'))+ ' MW')
        w5_total = w5_subtotal + error_value
        self.tw1.topLevelItem(3).setText(1, self.pre_check(w5_total))


        # tw1_2（同步即時欄 col=1）
        self._set(self.tw1_2, 1, (0,), w2_total)
        self._set(self.tw1_2, 1, (0, 0,), current_p['2H180':'1H350'].sum())
        self._set(self.tw1_2, 1, (0, 0, 0,), current_p['2H180'])
        self._set(self.tw1_2, 1, (0, 0, 1,), current_p['2H280'])
        self._set(self.tw1_2, 1, (0, 0, 2,), current_p['1H350'])
        self._set(self.tw1_2, 1, (0, 1,), current_p['4KA19'])
        self._set(self.tw1_2, 1, (0, 2,), current_p['4KB19':'4KB29'].sum())
        self._set(self.tw1_2, 1, (0, 2, 0,), current_p['4KB19'])
        self._set(self.tw1_2, 1, (0, 2, 1,), current_p['4KB29'])
        self._set(self.tw1_2, 1, (0, 3,), current_p['2KA41':'2KB41'].sum())
        self._set(self.tw1_2, 1, (0, 3, 0,), current_p['2KA41'])
        self._set(self.tw1_2, 1, (0, 3, 1,), current_p['2KB41'])
        self._set(self.tw1_2, 1, (0, 4,), current_p['W2'])

        self._set(self.tw1_2, 1, (1,), w3_total)
        self._set(self.tw1_2, 1, (1, 0,), current_p['AJ320'])
        self._set(self.tw1_2, 1, (1, 1,), current_p['5KA18':'5KB28'].sum())
        self._set(self.tw1_2, 1, (1, 1, 0,), current_p['5KA18'])
        self._set(self.tw1_2, 1, (1, 1, 1,), current_p['5KA28'])
        self._set(self.tw1_2, 1, (1, 1, 2,), current_p['5KB18'])
        self._set(self.tw1_2, 1, (1, 1, 3,), current_p['5KB28'])
        self._set(self.tw1_2, 1, (1, 2,), current_p['W3'])

        self._set(self.tw1_2, 1, (2,), w4_total)
        self._set(self.tw1_2, 1, (2, 0,), w41_main, pre_kwargs=dict(b=4))
        self._set(self.tw1_2, 1, (2, 1,), w4_utility)

        self._set(self.tw1_2, 1, (3,), w5_subtotal)
        self._set(self.tw1_2, 1, (3,0,), current_p['3KA14':'3KA15'].sum())
        self._set(self.tw1_2, 1, (3, 0, 0,), current_p['3KA14'])
        self._set(self.tw1_2, 1, (3, 0, 1,), current_p['3KA15'])
        self._set(self.tw1_2, 1, (3, 1,), current_p['3KA24':'3KA25'].sum())
        self._set(self.tw1_2, 1, (3, 1, 0,), current_p['3KA24'])
        self._set(self.tw1_2, 1, (3, 1, 1,), current_p['3KA25'])
        self._set(self.tw1_2, 1, (3, 2,), current_p['3KB12':'3KB28'].sum())
        self._set(self.tw1_2, 1, (3, 2, 0,), current_p['3KB12'])
        self._set(self.tw1_2, 1, (3, 2, 1,), current_p['3KB22'])
        self._set(self.tw1_2, 1, (3, 2, 2,), current_p['3KB28'])
        self._set(self.tw1_2, 1, (3, 3,), current_p['3KA16':'3KB27'].sum())
        self._set(self.tw1_2, 1, (3, 3, 0,), current_p['3KA16'])
        self._set(self.tw1_2, 1, (3, 3, 1,), current_p['3KA26'])
        self._set(self.tw1_2, 1, (3, 3, 2,), current_p['3KA17'])
        self._set(self.tw1_2, 1, (3, 3, 3,), current_p['3KA27'])
        self._set(self.tw1_2, 1, (3, 3, 4,), current_p['3KB16'])
        self._set(self.tw1_2, 1, (3, 3, 5,), current_p['3KB26'])
        self._set(self.tw1_2, 1, (3, 3, 6,), current_p['3KB17'])
        self._set(self.tw1_2, 1, (3, 3, 7,), current_p['3KB27'])
        self._set(self.tw1_2, 1, (3, 4,), current_p['2KA19':'2KB29'].sum())
        self._set(self.tw1_2, 1, (3, 4, 0,), current_p['2KA19'])
        self._set(self.tw1_2, 1, (3, 4, 1,), current_p['2KA29'])
        self._set(self.tw1_2, 1, (3, 4, 2,), current_p['2KB19'])
        self._set(self.tw1_2, 1, (3, 4, 3,), current_p['2KB29'])
        self._set(self.tw1_2, 1, (3, 5,), current_p['W5'])
        self._set(self.tw1_2, 1, (4,), current_p['WA'])
        # tw2_2（同步即時欄 col=1）
        self._set(self.tw2_2, 1, (0,), current_p['9H140':'9KB33'].sum(), pre_kwargs=dict(b=0))
        self._set(self.tw2_2, 1, (1,), current_p['AH120'], pre_kwargs=dict(b=0))
        self._set(self.tw2_2, 1, (2,), current_p['AH190'], pre_kwargs=dict(b=0))
        self._set(self.tw2_2, 1, (3,), current_p['AH130'], pre_kwargs=dict(b=0))
        self._set(self.tw2_2, 1, (4,), current_p['1H450'], pre_kwargs=dict(b=0))
        self._set(self.tw2_2, 1, (5,), current_p['1H360'], pre_kwargs=dict(b=0))
        # tw3_2（同步即時欄 col=1）
        self._set(self.tw3_2, 1, (0, ), current_p['2H120':'1H420'].sum())
        self._set(self.tw3_2, 1, (0, 0,), current_p['2H120':'2H220'].sum())
        self._set(self.tw3_2, 1, (0, 1,), current_p['5H120':'5H220'].sum())
        self._set(self.tw3_2, 1, (0, 2,), current_p['1H120':'1H220'].sum())
        self._set(self.tw3_2, 1, (0, 3,), current_p['1H320':'1H420'].sum())
        self._set(self.tw3_2, 1, (1, ), current_p['4KA18':'5KB19'].sum())
        self._set(self.tw3_2, 1, (1, 0,), current_p['4KA18'].sum())
        self._set(self.tw3_2, 1, (1, 1,), current_p['5KB19'].sum())
        self._set(self.tw3_2, 1, (2, ), current_p['4H120':'4H220'].sum())
        self._set(self.tw3_2, 1, (2, 0,), current_p['4H120'].sum())
        self._set(self.tw3_2, 1, (2, 1,), current_p['4H220'].sum())

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
        查詢指定「日期」的 15 分鐘需量並更新右側表格（tableWidget_2）。

        行為
        ----
        - 依 UI 選擇決定計算來源：
          * kWh 模式（radioButton_5 勾選）：對兩條 kWh tag 相加後乘以 4，直接視為 15 分鐘需量
            （等同將每 15 分鐘累積電量換算為 MW 平均功率）。
          * P 模式（radioButton_5 未勾選）：以 PI summary="AVERAGE"、interval='6s' 取得 1~5 秒粒度的
            平均功率，先將負值以 0 取代，再以 resample('15T').mean() 轉成 15 分鐘平均功率，
            接著將兩迴路相加得到該 15 分鐘需量。

        表格呈現
        --------
        - 每天 96 個區段分 6 欄顯示（每欄 16 列）；偶數欄顯示時間、奇數欄顯示該段需量。
        - 若某段為未來時間（區段結束時刻 > 現在），需量以紅字顯示；其餘為藍字。
        - 該段無數值（NaN）時，該格顯示為空字串。

        注意
        ----
        - P 模式會先以 clip(lower=0) 把負值視為 0 才取平均，以符合需量邏輯。
        - kWh 模式不進行重採樣，直接以（1510 + 1520）* 4 作為每 15 分鐘需量列。
        """
        st = pd.Timestamp(str(self.dateEdit.date().toPyDate()))
        et = st + pd.offsets.Day(1)

        # 根據radioButton_5，判斷用kwh 或p 計算需量。
        if self.radioButton_5.isChecked():
            tags=('W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11')
            raw_result = pi_client.query(st=st, et=et, tags=tags)
            raw_result.insert(0, 'TPC', (raw_result.iloc[:, 0] + raw_result.iloc[:, 1]) * 4)
            demand_15min = raw_result
        else:
            tags=('W511_MS1/161KV/1510/P', 'W511_MS1/161KV/1520/P')
            raw_result = pi_client.query(st=st, et=et, tags=tags, summary="AVERAGE", interval='6s')
            raw_result = raw_result.clip(lower=0)
            raw_data = raw_result.resample('15T').mean()
            raw_data.insert(0, 'TPC', (raw_data.iloc[:, 0] + raw_data.iloc[:,1]))
            demand_15min = raw_data

        for j in range(6):          # 1
            for i in range(16):
                item1 = QtWidgets.QTableWidgetItem(pd.Timestamp(demand_15min.index[i + j * 16]).strftime('%H:%M'))  #2
                font = QtGui.QFont()
                font.setPointSize(10)
                item1.setFont(font)         # 3
                self.tableWidget_2.setItem(i, 0 + j * 2,item1)
                self.tableWidget_2.item(i, 0 + j * 2).setTextAlignment(4 | 4)       # 4

                if pd.isnull(demand_15min.iloc[i + j * 16, 0]):             # 5
                    item2 = QtWidgets.QTableWidgetItem(str(''))
                else:
                    item2 = QtWidgets.QTableWidgetItem(str(round(demand_15min.iloc[i + j * 16,0], 3)))
                if pd.Timestamp.now() < (demand_15min.index[i + j * 16].tz_localize(None) + pd.offsets.Minute(15)):
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

    def calculate_demand(self, e_date_time):
        """
        計算 CBL（基準用電量）所需的「多個參考日、指定時段」之 15 分鐘需量，並回傳為 DataFrame。

        參數
        ----
        e_date_time : pandas.Timestamp
            欲計算的「結束時間」；開始時間由 UI 的日期與時間（dateEdit_2, timeEdit）決定，
            長度由 spinBox_2（小時數）決定。當現在時間已超過 e_date_time 時，參考日區間向後平移一天。

        計算方式
        --------
        - 參考日：呼叫 define_cbl_date() 取得一組日期清單（由 UI 決定天數／指定日）。
        - kWh 模式（radioButton_5 勾選）：
            讀取 1510/1520 兩條 kWh tag，將同時刻兩者相加後乘以 4，視為 15 分鐘需量序列。
        - P 模式（radioButton_5 未勾選）：
            以 summary="AVERAGE", interval='6s' 讀取功率，將負值剪成 0，並做 resample('15T').mean()，
            再把兩條迴路相加為該時刻之 15 分鐘需量。

        回傳
        ----
        pandas.DataFrame
            欄為各參考日（日期），列為該參考日中「指定時段」涵蓋的 15 分鐘需量。
            該結果通常會再被 mean(axis=0, skipna=True) 取得 CBL。

        備註
        ----
        - P 模式先 clip(lower=0) 再平均，確保需量不被負值拉低。
        - 取樣起訖時刻會依 UI 的開始時間與時長（不可跨日）逐一切片組成。
        """
        if pd.Timestamp.now() > e_date_time:  # 1
            cbl_date = self.define_cbl_date(e_date_time.date() + pd.offsets.Day(1))
        else:
            cbl_date = self.define_cbl_date(e_date_time.date())

        # 根據radioButton_5，判斷用kwh 或p 計算需量。
        if self.radioButton_5.isChecked():
            tags=('W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11')
            # 2
            buffer2 = pi_client.query(st=pd.Timestamp(cbl_date[-1]),
                                      et=pd.Timestamp(cbl_date[0] + pd.offsets.Day(1)), tags=tags)
            row_data = (buffer2.iloc[:, 0] + buffer2.iloc[:, 1]) * 4  # 3
        else:
            tags=('W511_MS1/161KV/1510/P', 'W511_MS1/161KV/1520/P')
            # 2
            buffer2 = pi_client.query(st=pd.Timestamp(cbl_date[-1]),
                                         et=pd.Timestamp(cbl_date[0] + pd.offsets.Day(1)),
                                         tags=tags, summary="AVERAGE", interval='6s')
            buffer2 = buffer2.clip(lower=0)
            buffer2 = buffer2.resample('15T').mean()
            row_data = (buffer2.iloc[:, 0] + buffer2.iloc[:, 1])  # 3

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
        self.set_tablewidget5_header()

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

    def set_tablewidget5_header(self):
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

    @staticmethod
    def get_period_background(period):
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

    @staticmethod
    def get_benefit_colors(value) -> Tuple[str, str]:  # 用 typing.Tuple 替代 tuple[str, str]
        return ('blue', '#E6F0FF') if value >= 0 else ('red', '#FBE4E4')

    @staticmethod
    def build_ng_table4_tooltip(name: str, ng_cost_versions: list) -> str:
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

    @staticmethod
    def build_cost_cell_tooltip(ng_cost: float, tg_cost: float) -> str:
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

    @staticmethod
    def build_cost_tooltip(ng_cost_list):
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

    @staticmethod
    def build_price_tooltip(period, ver_list, is_sale=False):
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

    @staticmethod
    def auto_resize(table: QtWidgets.QTableWidget, min_height: int = 60):
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

    @staticmethod
    def pre_check(pending_data, b=1, c='power'):
        """
        此函式用來判顯示在tree,table widget  的即時資料，是否有資料異常、設備沒有運轉或停機的狀況 (數值接近 0)
        :param c: 用來判斷是燃氣或電力的類別
        :param pending_data:要判斷的數值。
        :param b:若數值接近 0，預設回傳'停機'的述述。
        :return: 回傳值為文字型態。
        """
        describe = ['--', '停機', '資料異常', '未使用', '0 MW', '未發電']
        if pd.isnull(pending_data):
            return describe[2]
        if pending_data > 0.1:
            if c == 'gas':
                return str(format(round(pending_data, 1), '.1f'))
            elif c == 'h':
                return str(format(round(pending_data, 2), '.2f'))
            else:
                return str(format(round(pending_data, 2), '.2f')) + ' MW'
        else:
            return describe[b]

    @staticmethod
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

    @staticmethod
    def _item_at(tree, path):
        """
            配合_set() 實例方法，用來簡化realtime_update_to_tws、history_update_to_tws 裡，
            大量重複的樹狀節點更新碼。
        參數：
            tree:
                用來接收 QTreeWidget 物件。
            path:
                每一層樹狀結構的index，例如 (0, 3, 1) 代表 top(0) -> child(3) -> child(1)。
        行為：

        回傳：
            指定層別的特定項目
        """
        item = tree.topLevelItem(path[0])
        for idx in path[1:]:
            item = item.child(idx)
        return item

    def _set(self, tree, col, path, value, *, avg=False, pre_kwargs=None, suffix=""):
        """
            配合_item_at 靜態方法，用來簡化realtime_update_to_tws、history_update_to_tws 裡，
            大量重複的樹狀節點更新碼。
        參數：
            tree:
                用來接收 QTreeWidget 物件。
            col:
                項目對應的 column index
            path:
                項目對應在樹狀結構的index，例如 (0, 3, 1) 代表 top(0) -> child(3) -> child(1)。
            value:
                接收要更新的內容
            avg:
                False 走 self.pre_check，True 走 self.pre_check2
            pre_kwargs:
                給 pre_check/pre_check2 的參數（如 b=0）
            suffix:
                額外字尾，例如 ' MW'
        行為：
            統一 setText + pre_check / pre_check2
        回傳：
            無
        """
        pre_kwargs = pre_kwargs or {}
        fmt = self.pre_check2 if avg else self.pre_check
        text = fmt(value, **pre_kwargs)
        if suffix:
            text = f"{text}{suffix}"
        self._item_at(tree, path).setText(col, text)

if __name__ == "__main__":
    sys.excepthook = handle_uncaught
    pi_client = PIClient()
    app = QtWidgets.QApplication(sys.argv)
    myWin = MyMainWindow()
    myWin.show()

    # 包try/except 的啟動事件迴圈
    try:
        exit_code = app.exec()
    except Exception:
        # 任何在事件迴圈裡冒出的例外，都記一次錯誤日誌，但不終止程式 (如果想要強制結束，就再 raise)
        logger.error("Exception in Qt event loop", exc_info=True)
        exit_code = 1
    sys.exit(exit_code)
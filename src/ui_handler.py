
from PyQt6 import QtCore
from PyQt6 import QtWidgets
from logging_utils import get_logger

logger = get_logger(__name__)

def setup_ui_behavior(ui):
    """
    初始化並綁定主視窗的 UI 行為與預設狀態。

    功能總覽
    --------
    - 事件連結：
      * 主要按鈕：查詢、加入/移除清單、需求查詢、效益評估…等
      * Tree/Table：雙擊、選取變更、展開/收合等
      * 控制元件：捲動條、日期/時間編輯器、CheckBox 狀態切換
      * QAction：離開、開發者選單
    - 樣式初始化：
      * TreeWidget / TableWidget 美化與欄寬/欄位設定
      * 狀態列字體與多行訊息 Label（置於 statusBar）
    - 預設值設定：
      * 日期與時區、CheckBox 初值、SpinBox 預設
    - 啟動預設查詢與背景緒：
      * `define_cbl_date()`、`query_cbl()`、`query_demand()`
      * 啟動 start_schedule_thread() 與 `start_dashboard_thread()`（即時值、排程）
    - 其他可選：
      * 若存在 `trend_chart`，插入預設容器
      * 若存在 `initialize_cost_benefit_widgets()`，初始化效益分析表格

    Parameters
    ----------
    ui : QMainWindow
        主視窗實例；此函式內直接操作其屬性與成員方法（副作用性初始化）。

    Notes
    -----
    - 本函式不回傳值，純粹以副作用設定 UI 與啟動必要的背景工作。
    - 某些訊號（例如 DashboardThread → 主執行緒的圖表資料）可在主程式依需求以 QueuedConnection
      額外連線，以確保跨執行緒安全。

    """
    # ===== 按鈕事件連結 =====
    ui.pushButton.clicked.connect(ui.query_cbl)
    ui.pushButton_2.clicked.connect(ui.add_item_to_cbl_list)
    ui.pushButton_3.clicked.connect(ui.remove_item_from_cbl_list)
    ui.pushButton_4.clicked.connect(ui.query_demand)
    ui.pushButton_5.clicked.connect(ui.benefit_appraisal)

    # ===== Tree/Table 選擇與輸入事件 =====
    ui.listWidget.doubleClicked.connect(ui.remove_item_from_cbl_list)
    ui.spinBox_2.valueChanged.connect(ui.tz_changed)
    ui.timeEdit.dateTimeChanged.connect(ui.tz_changed)
    ui.tableWidget_2.itemSelectionChanged.connect(ui.handle_selection_changed)

    # ===== Checkbox 狀態變更事件 =====
    ui.checkBox.stateChanged.connect(ui.check_box_event)
    ui.checkBox_2.stateChanged.connect(ui.check_box2_event)

    # ===== TreeWidget 展開與收合事件 =====
    ui.tw1.itemExpanded.connect(ui.tw1_expanded_event)
    ui.tw1.itemCollapsed.connect(ui.tw1_expanded_event)
    ui.tw3.itemExpanded.connect(ui.tw3_expanded_event)
    ui.tw3.itemCollapsed.connect(ui.tw3_expanded_event)

    # ===== Tree/Table 樣式初始化 =====
    ui.beautify_tree_widgets()
    ui.beautify_table_widgets()
    ui.tws_init()

    # ===== ScrollBar 與 DateEdit 控制 =====
    ui.horizontalScrollBar.valueChanged.connect(ui.scroller_changed_event)
    ui.dateEdit_3.dateChanged.connect(ui.date_edit3_user_change)
    # 直接設定calendarwidget 的最大日期，減少在程式中預防未來日期的的撰寫
    ui.dateEdit_3.setMaximumDate(QtCore.QDate.currentDate())

    # ===== 主視窗下面 status bar (狀態欄) 相關設定 =====
    font = ui.statusBar().font()
    font.setPointSize(12)
    ui.statusBar().setFont(font)

    ui.multiLineLabel = QtWidgets.QLabel(ui)
    ui.multiLineLabel.setWordWrap(True)
    ui.statusBar().addWidget(ui.multiLineLabel, 1)

    # ===== 設定初始日期與時間元件 =====
    ui.dateEdit.setDate(QtCore.QDate().currentDate())
    ui.dateEdit_2.setDate(QtCore.QDate().currentDate())
    ui.checkBox_2.setChecked(False)
    ui.spinBox.setValue(5)
    ui.spinBox_2.setValue(4)

    # ===== 啟動預設查詢行為 =====
    ui.define_cbl_date(QtCore.QDateTime.currentDateTime().date().toPyDate())
    ui.query_cbl()
    ui.query_demand()

    # ===== 啟動 QThread 開始背景任務 (連續更新即時值、產線排程） =====
    ui.start_schedule_thread()
    ui.start_dashboard_thread()

    # ===== TrendChart 嵌入設定（如果需要） =====
    if hasattr(ui, 'trend_chart'):
        ui.verticalLayout.addWidget(ui.trend_chart)

    # ===== 初始化效益分析表格（需要手動設定表格樣式） =====
    if hasattr(ui, 'initialize_cost_benefit_widgets'):
        ui.initialize_cost_benefit_widgets()

    # ====== QAction 綁定棤的設定 =======
    ui.actionExit.triggered.connect(ui.close)
    ui.develop_option.triggered.connect(ui.develop_option_event)

    # ==== 連接 signal -> slot (主執行緒)
    # 連接 signal → slot（主執行緒）
    #if ui.dashboard_thread is not None:
    #    ui.dashboard_thread.sig_pie_series.connect(
    #        ui._on_pie_series,
    #        QtCore.Qt.ConnectionType.QueuedConnection
    #    )


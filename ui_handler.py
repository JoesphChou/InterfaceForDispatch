from typing import Optional

from PyQt6 import QtCore
from PyQt6 import QtWidgets

from logging_utils import get_logger
logger = get_logger(__name__)

def setup_ui_behavior(ui):
    """
    將 PyQt UI 中的事件綁定與元件初始化統一管理，包含：
    - 按鈕連結
    - Tree/Table 樣式設定
    - Thread 初始化
    - 預設值設定
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

    # ===== TrendChart 嵌入設定（如果需要） =====
    if hasattr(ui, 'trend_chart'):
        ui.verticalLayout.addWidget(ui.trend_chart)

    # ===== 啟動 QThread 開始背景任務 (連續更新即時值、產線排程） =====
    ui.start_schedule_thread()
    ui.start_dashboard_thread()

    # ===== 初始化效益分析表格（需要手動設定表格樣式） =====
    if hasattr(ui, 'initialize_cost_benefit_widgets'):
        ui.initialize_cost_benefit_widgets()

    # ====== QAction 綁定棤的設定 =======
    ui.actionExit.triggered.connect(ui.close)
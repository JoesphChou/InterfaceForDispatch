import PIconnect as PI
from PyQt6 import QtCore, QtWidgets, QtGui
from src.UI import Ui_Form
import sys
import pandas as pd
import time

time_offset_with_OSAKAI = 30    # 用來近似 OSAKI 時間用的參數(秒數)

def timeit(func):
    print('接到 func', func.__name__)
    def wrapper(*args, **kwargs):
        print('幫忙代入 args', args)
        print('幫忙代入 kwargs', kwargs)
        s = time.time()
        func(*args, **kwargs)
        print(func.__name__, 'total time', time.time()-s)
    return wrapper

class MyMainForm(QtWidgets.QMainWindow, Ui_Form):

    def __init__(self, parent=None):
        super(MyMainForm, self).__init__()
        self.setupUi(self)
        self.pushButton.clicked.connect(self.button_caculate)
        self.pushButton_2.clicked.connect(self.addListItem)
        self.pushButton_3.clicked.connect(self.removeListItem1)
        self.pushButton_4.clicked.connect(self.button_query)
        self.dateEdit.setDate(QtCore.QDate().currentDate())
        self.dateEdit_2.setDate(QtCore.QDate().currentDate())
        self.spinBox.setValue(5)
        self.spinBox_2.setValue(4)
        self.listWidget.doubleClicked.connect(self.removeListItem1)
        self.spinBox_2.valueChanged.connect(self.tzchanged)
        self.timeEdit.dateTimeChanged.connect(self.tzchanged)
        self.tableWidget_2.itemSelectionChanged.connect(self.handle_selection_changed)
        self.special_dates = pd.read_excel('.\parameter.xlsx', sheet_name=1)
        self.defind_CBL_date(pd.Timestamp.now().date())   # 初始化時，便立即找出預設的cbl參考日，並更新在list widget 裡
        self.button_caculate()
        self.button_query()

    def handle_selection_changed(self):
        """
        1. 以list的方式返回被選擇的item
        2. 排除非需量或空白字的 cell
        :return:
        """
        a = self.tableWidget_2.selectedItems()  # 1
        sum = list()
        for i in range(len(a)):     # 2
            if (a[i].column() % 2 != 0) & (a[i].text() != ''):
                sum.append(a[i].text())
        b = pd.Series(sum, dtype=float)
        self.label_6.setText(str(b.mean()))
        self.label_8.setText(str(len(b)))

    def button_query(self):
        """
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
        raw_data = self.query_PI(st=st, et=et, tags=tags)
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
                    item2 = QtWidgets.QTableWidgetItem(str(round(raw_data.iloc[i + j * 16,0], 3)))     # 四捨五入至小數第3位
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
        newitem = QtWidgets.QTableWidgetItem('test')
        self.tableWidget.setItem(0,0,newitem)               # 設定某表格內容
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

    def button_caculate(self):
        start_date_time = pd.Timestamp(str(self.dateEdit_2.date().toPyDate() +
                                           pd.offsets.Hour(self.timeEdit.time().hour())))
        end_date_time = start_date_time + pd.offsets.Hour(self.spinBox_2.value())
        self.tzchanged()    # 調整timezone
        if self.radioButton_2.isChecked():
            if self.listWidget.count() == 0:
                self.showBox(content='未指定任何參考日')
                return
            if (self.listWidget.count !=0) & (self.spinBox.value() != self.listWidget.count()):
                self.showBox(content='參考日數量與天數不相符')
                return
        a = pd.Timestamp(str(self.timeEdit.time().toString()))
        b = a + pd.offsets.Hour(self.spinBox_2.value())
        if b.day > a.day:
            self.showBox(content='時間長度不可跨至隔天')
            return

        """ 設定表格
            1. 依CBL 參考天數，設定表格column 數量
            2. 將第2row 的表格全部合併
            3. 將計算好的CBLs指定至特定表格位置，並且將內容置中對齊
            4. 設定column、row 的名稱    
            5. 將計算好的CBL 顯示於第 2 row，並且將內容置中對齊
            6. 將表格的高度、寬度自動依內容調整   
        """
        self.tableWidget.setColumnCount(self.spinBox.value())       # 1
        self.tableWidget.setSpan(1, 0, 1, self.spinBox.value())     # 2
        demands = self.caculate_demand(s_date_time= start_date_time, e_date_time= end_date_time) # DataFrame
        cbls = demands.mean(axis=0, skipna=True)    # Series
        header_label = list()
        for i in range(len(demands.columns)):
            header_label.append(str(demands.columns[i]))
            item = QtWidgets.QTableWidgetItem(str(round(cbls[i],3)))        # 3-1
            self.tableWidget.setItem(0,i,item)                              # 3-2
            self.tableWidget.item(0,i).setTextAlignment(4|4)                # 3-3
        self.tableWidget.setHorizontalHeaderLabels([label for label in header_label])   # 4-1
        self.tableWidget.setVerticalHeaderLabels(['平均值','CBL'])                       # 4-2
        item = QtWidgets.QTableWidgetItem(str(round(cbls.mean(),3)))        #5-1
        self.tableWidget.setItem(1,0,item)                                  #5-2
        self.tableWidget.item(1,0).setTextAlignment(4|4)                    #5-3
        self.tableWidget.resizeColumnsToContents()   #6
        self.tableWidget.resizeRowsToContents()      #6

    def query_PI(self,st, et, tags, exation_type = 0):
        """
            1. 從 PI 取出的 timestamp 時區改成 GMT+8
            2. 用 PI.PIServer().search 找出tag 對應的PIPoint，回傳的結果是list 型態。
               將該結果從list 提出，並新增到points 的list 中。
            3. 針對每一個PIPoint 透過 summaries 的方法，依exation_type 內容，決定特定區間取出值為何種形式。
               此方法回傳的資料為DataFrame 型態
            4. 將每筆DataFrame 存成list 之前，將資料型態從Object -> float，若有資料中有文字無法換的，則用NaN 缺失值取代。
               這邊使用的column名稱 ('RANGE')，必須視依不同的exation type 進行調整。
            5. 將list 中所有的 DataFrame 合併為一組新的 DataFrame 資料
            6. 把原本用來做index 的時間，將時區從tz aware 改為 native，並加入與OSAKI 時間差參數進行調整。
        :param st:  區間起始點的日期、時間
        :param et:  區間結束點的日期、時間
        :param tags:  list。 要查調的所有 tag
        :param exation_type: 預設為1。  0：PI.PIConsts.SummaryType.RANGE
                                      1：PI.PIConsts.SummaryType.MAXIMUM
                                      2：PI.PIConstsSummaryType.MINIMUM
        :return: 將結果以 DataFrame 格式回傳。 shape(資料數量, tag數量)
        """
        st = st - pd.offsets.Second(time_offset_with_OSAKAI)
        et = et - pd.offsets.Second(time_offset_with_OSAKAI)
        PI.PIConfig.DEFAULT_TIMEZONE = 'Asia/Taipei'        #1
        summarytype= [PI.PIConsts.SummaryType.RANGE, PI.PIConsts.SummaryType.MAXIMUM, PI.PIConsts.SummaryType.MINIMUM]
        with PI.PIServer() as server:
            points = list()
            for tag_name in tags:
                points.append(server.search(tag_name)[0])   #2
            buffer = list()
            for x in range(len(points)):
                data = points[x].summaries(st, et, '15m', summarytype[exation_type])     # 3
                data['RANGE'] = pd.to_numeric(data['RANGE'], errors='coerce')                   # 4
                buffer.append(data)
            raw_data = pd.concat([s for s in buffer], axis=1)                                   # 5
            raw_data.set_index(raw_data.index.tz_localize(None)
                               + pd.offsets.Second(time_offset_with_OSAKAI),inplace = True)     # 6
        return raw_data

    def caculate_demand(self, s_date_time, e_date_time):
        """
            1. 根據目前時間是否超出取樣時間的最後一段，決定呼叫 defind_CBL_date 函式的參數，取得一組list，list 中存有CBL 參考日期
            2. 起始時間為參考日最早的一天，結束時間為參考日最後一天+1
            3. buffer2 的第 0、1 Column 進行相加後乘4的運算，並把結果將 Series的型態存在row_data
        :param s_date_time、e_date_time 傳入的參數數為TimeStamp，為完整的起時和結束的日期+時間
        :return: 將CBL 參考日指定時段的平均需量，用 DataFrame 的方式回傳
        """
        if pd.Timestamp.now() > e_date_time:  # 1
            cbl_date = self.defind_CBL_date(e_date_time.date() + pd.offsets.Day(1))
        else:
            cbl_date = self.defind_CBL_date(e_date_time.date())
        tags = ['W511_MS1/161KV/1510/kwh11', 'W511_MS1/161KV/1520/kwh11']
        # 2
        buffer2 = self.query_PI(st=pd.Timestamp(cbl_date[-1]),
                                et=pd.Timestamp(cbl_date[0] + pd.offsets.Day(1)), tags=tags)
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
        s_time = str(period_start[0].time())        # 2
        e_time = str((period_start[0] + pd.offsets.Minute((self.spinBox_2.value() * 4 - 1) * 15)).time())

        demands_buffer = list()
        for i in range(self.spinBox.value()):
            s_point = str(period_start[i])
            e_point = str(period_start[i] + pd.offsets.Minute((self.spinBox_2.value() * 4 - 1) * 15))
            demands_buffer.append(row_data.loc[s_point: e_point])  # 3
            demands_buffer[i].rename(cbl_date[i].date(), inplace=True, copy=False)  # 4
            demands_buffer[i].reset_index(drop=True, inplace=True)  # 5
            demands_buffer[i].index = [a for a in (pd.date_range(s_time, e_time, freq='15min').time)]  # 6
        demands = pd.concat([s for s in demands_buffer], axis=1)
        return demands

    def defind_CBL_date(self, date):    #回傳list
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
        :param pending_date: 待判斷的日期 (dtype:TimeStamnp
        :return: 用 bool 的方式回傳是或不是
        """
        special_date = pd.concat([self.special_dates.iloc[:,0], self.special_dates.iloc[:,1].dropna()],
                                 axis=0, ignore_index=True)
        for sdate in special_date:      # 將傳進來的日期與special_date 逐一比對，有一樣的就回傳true
            if pending_date.date() == sdate:
                return True
        return False

    def removeListItem1(self):
        selected = self.listWidget.currentRow() # 取得目前被點撃item 的index
        self.listWidget.takeItem(selected) # 將指定index 的item 刪除

    def addListItem(self):
        pending_date = pd.Timestamp(self.dateEdit_2.date().toString())
        if pending_date.date() >= pd.Timestamp.today().date():      # datetime格式比較
            self.showBox(content='不可指定今天或未來日期作為CBL參考日期！')
            return
        for i in range(self.listWidget.count()):
            if pending_date == pd.Timestamp(self.listWidget.item(i).text()):
                self.showBox(content='不可重複指定同一天為CBL參考日期！')
                return
        self.listWidget.addItem(str(self.dateEdit_2.date().toPyDate()))  #Add special day to listWidget

    def tzchanged(self):
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

    def showBox(self, content):
        mbox = QtWidgets.QMessageBox(self)
        mbox.warning(self, '警告', content)
        """
        mbox.setText(content)   # 通知文字
        mbox.addButton(QtWidgets.QMessageBox.StandardButton.Ok)
        mbox.setIcon(QtWidgets.QMessageBox.Icon.Question)  # 加入問號 icon
        mbox.exec()
        """
    """     # 目前此功能如果遇到有合併的儲存格時會出錯，所以暫時先不使用
    def keyPressEvent(self, event):
        # 偵測鍵盤有輸入"Ctrl + C" 按鍵後，將tablewidget 所選的內容，複制到系統的clipboard。
        super().keyPressEvent(event)
        if event.key() == QtCore.Qt.Key.Key_C and (event.modifiers() & QtCore.Qt.KeyboardModifier.ControlModifier):
            copied_cells = sorted(self.tableWidget.selectedIndexes())
            max_column = copied_cells[-1].column()
            max_row = copied_cells[-1].row()

            copy_text = pd.DataFrame(np.full(shape=(max_row + 1, max_column + 1), fill_value=np.nan))
            copy_text = copy_text.astype('string')

            for c in range(0, len(copied_cells)):
                copy_text.iat[copied_cells[c].row(), copied_cells[c].column()] = self.tableWidget.item(
                    copied_cells[c].row(), copied_cells[c].column()).text()
            copy_text.columns = [self.tableWidget.horizontalHeaderItem(n).text() for n in range (max_column +1)]
            # copy_text 這個DF 的column 名稱 引用table widget  -->待debug
            print(copy_text)
            print(type(copy_text))
            copy_text.to_clipboard(excel=True)
    """

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    myWin = MyMainForm()
    myWin.show()
    sys.exit(app.exec())
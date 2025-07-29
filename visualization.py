"""
visualization.py

提供繪製趨勢圖與互動工具的功能，包含:
  - plot_tag_trends: 在單圖中疊多條時序曲線
  - CustomToolbar: 延伸 NavigationToolbar2QT 以修正儲存對話框行為
  - TrendWindow: 簡易 QMainWindow 包裝 matplotlib Figure
  - TrendChartCanvas: 支援滑鼠互動提示的 FigureCanvas
"""
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas, NavigationToolbar2QT
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from logging_utils import get_logger
import pandas as pd
from typing import List, Optional, Tuple
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QFileDialog

logger = get_logger(__name__)

def plot_tag_trends(
    df: pd.DataFrame,
    tags: List[str],
    *,
    title: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 6),
    show_legend: bool = True,
):
    """
    在同一張圖上繪製多個 tag 的時間序列曲線。

    Args:
        df (pd.DataFrame): 索引為 Timestamp 的資料表，欄位為各 tag。
        tags (List[str]): 欲繪製的欄位名稱列表。

    Keyword Args:
        title (Optional[str]): 圖表標題，預設 None。
        figsize (Tuple[int,int]): 圖表尺寸 (寬, 高)，單位為英吋，預設 (12,6)。
        show_legend (bool): 是否顯示圖例，預設 True。

    Returns:
        Tuple[Figure, Axes]: 繪製完成的 matplotlib Figure 和 Axes 物件。
    """
    fig, ax = plt.subplots(figsize=figsize)
    for tag in tags:
        if tag not in df.columns:
            continue
        ax.plot(df.index, df[tag], label=tag)

    # 軸線格式化
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d\n%H:%M"))
    ax.set_xlabel("Datetime")
    ax.set_ylabel("Value")
    if title:
        ax.set_title(title)

    if show_legend:
        ax.legend(loc="best")

    # 加網格
    ax.grid(True, which='major', linestyle='--', alpha=0.5)     # 主格線
    ax.minorticks_on()                                          # 開啟次要刻度
    ax.grid(True, which='minor', linestyle=':', alpha=0.3)      # 次格線

    fig.autofmt_xdate()
    fig.tight_layout()
    return fig, ax

class CustomToolbar(NavigationToolbar2QT):
    """
    CustomToolbar 類別，繼承 NavigationToolbar2QT，
    修正儲存對話框被遮蔽問題。
    """
    def save_figure(self, *args):
        """
        顯示檔案儲存對話框並將當前 Figure 存檔。

        Args:
            *args: 原方法參數（未使用）。
        """
        filename, _ = QFileDialog.getSaveFileName(
            parent=self,
            caption="Save Figure",
            directory="",
            filter="PNG Files (*.png);;All Files (*)",
            options=QFileDialog.Option.DontUseNativeDialog
        )

        if filename:
            self.canvas.figure.savefig(filename)

class TrendWindow(QtWidgets.QMainWindow):
    """
    TrendWindow 類別，用於在獨立視窗中顯示趨勢圖。

    Args:
        fig (Figure): 要顯示的 matplotlib Figure 物件。
        parent (QObject, optional): 父物件，預設 None。
    """
    def __init__(self, fig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("用電特性趨勢圖")
        # 建立中央容器
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        layout = QtWidgets.QVBoxLayout(central_widget)

        # 建立 Matplotlib Canvas 與 Toolbar
        canvas = FigureCanvas(fig)
        toolbar = CustomToolbar(canvas, self)

        layout.addWidget(toolbar)
        layout.addWidget(canvas)

        self.resize(900, 500)

class TrendChartCanvas(FigureCanvas):
    """
    TrendChartCanvas 類別，繼承 FigureCanvas，
    支援滑鼠互動提示 (tooltip) 的趨勢圖繪製。
    """
    def __init__(self, parent=None, width=6, height=3, dpi=100):
        """
        初始化繪圖畫布並設定字型與顯示參數。

        Args:
            parent (QWidget, optional): 父物件，預設 None。
            width (float): 圖形寬度 (英吋)，預設 6。
            height (float): 圖形高度 (英吋)，預設 3。
            dpi (int): 圖形解析度 (每英吋點數)，預設 100。
        """
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.ax = fig.add_subplot(111)
        super().__init__(fig)
        self.setParent(parent)
        plt.rcParams['font.family'] = 'Microsoft JhengHei'  # 微軟正黑體
        plt.rcParams['axes.unicode_minus'] = False  # 支援負號正確顯示


    def plot_from_dataframe(self, df):
        """
        根據 DataFrame 繪製趨勢圖並設定互動提示 (tooltip)。

        Args:
            df (pd.DataFrame): 必須包含 '原始TPC' 與 '即時TPC' 欄位。
        """
        if not {'原始TPC', '即時TPC'}.issubset(df.columns):
            self.ax.clear()
            self.ax.set_title("資料格式錯誤：缺少 '原始TPC' 或 '即時TPC'")
            self.draw()
            return

        self.df = df
        self.ax.clear()
        self.setup_base_plot(df)
        self.setup_tooltips(df)
        self.draw()

    def setup_base_plot(self, df):
        """
        建立基本圖形元素，包括填色、折線與坐標格式化。

        Args:
            df (pd.DataFrame): 包含 '原始TPC' 與 '即時TPC' 的資料表。
        """
        COLOR_UNCOMP = '#4FC3F7'  # 改成亮藍
        COLOR_COMP = '#FF7043'  # 改成橘紅
        self.x = df.index
        self.y1 = df['原始TPC'].astype(float).to_numpy()
        self.y2 = df['即時TPC'].astype(float).to_numpy()

        self.ax.fill_between(self.x, self.y1, color=COLOR_UNCOMP, alpha=0.6, label='未補NG')
        self.line1, = self.ax.plot(self.x, self.y1, alpha=0, picker=5)

        self.line2, = self.ax.plot(self.x, self.y2, color=COLOR_COMP, linewidth=1, label='有補NG')

        self.ax.set_title("台電供電量 (未補NG VS 有補NG)")
        self.ax.set_xlabel("時間")
        self.ax.set_ylabel("電量 (MW)")
        self.ax.grid(True)
        self.ax.legend()

        locator = mdates.AutoDateLocator()
        self.ax.xaxis.set_major_locator(locator)
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        self.ax.set_xlim(df.index[0], df.index[-1])

        y_all = np.concatenate([self.y1, self.y2])
        y_min = np.min(y_all)
        y_max = np.max(y_all)
        padding = (y_max - y_min) * 0.1 if y_max != y_min else 1
        self.ax.set_ylim(y_min - padding, y_max + padding)

        self.figure.autofmt_xdate()

        self.vline = self.ax.axvline(df.index[0], color='black', linestyle='--', linewidth=0.8, alpha=0.5)

    def setup_tooltips(self, df):
        """
        設定滑鼠互動提示 (tooltip) 的文字框與樣式。

        Args:
            df (pd.DataFrame): 原始資料表，用於動態計算提示內容。
        """
        self._tooltip_time = self.ax.text(
            0.5, -0.12, '', transform=self.ax.transAxes,
            ha='center', va='top', fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", facecolor='black', edgecolor='black'),
            color='white'
        )

        self._tooltip1 = self.ax.annotate('', xy=(0, 0), xytext=(10, -10), textcoords='offset points',
                                          bbox=dict(boxstyle="round", fc="white", ec="gray", lw=0.5), fontsize=9,
                                          visible=False)
        self._tooltip2 = self.ax.annotate('', xy=(0, 0), xytext=(10, -50), textcoords='offset points',
                                          bbox=dict(boxstyle="round", fc="white", ec="gray", lw=0.5), fontsize=9,
                                          visible=False)

        self.figure.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

    def on_mouse_move(self, event):
        if not event.inaxes:
            self._tooltip1.set_visible(False)
            self._tooltip2.set_visible(False)
            self._tooltip_time.set_text('')
            self.draw()
            return

        try:
            x_pos = mdates.num2date(event.xdata).replace(tzinfo=None)
        except Exception:
            return

        if x_pos < self.x[0] or x_pos > self.x[-1]:
            self._tooltip1.set_visible(False)
            self._tooltip2.set_visible(False)
            self._tooltip_time.set_text('')
            self.draw()
            return

        idx = np.searchsorted(self.x, x_pos)
        if idx >= len(self.x):
            idx = len(self.x) - 1

        x_val = self.x[idx]
        y1_val = self.y1[idx]
        y2_val = self.y2[idx]

        self.vline.set_xdata([x_val])

        self._tooltip_time.set_text(x_val.strftime('%m/%d %H:%M'))
        self._tooltip_time.set_position(
            ((mdates.date2num(x_val) - mdates.date2num(self.x[0])) / 
             (mdates.date2num(self.x[-1]) - mdates.date2num(self.x[0])), -0.12)
        )
        y_mid = (y1_val + y2_val) / 2
        self._tooltip1.xy = (x_val, y_mid)
        self._tooltip1.set_text(f"未補NG：{y1_val:,.1f} MW")
        self._tooltip1.set_fontsize(10)
        self._tooltip1.set_fontweight('bold')
        self._tooltip1.set_visible(True)

        self._tooltip2.xy = (x_val, y_mid)
        self._tooltip2.set_text(f"有補NG：{y2_val:,.1f} MW")
        self._tooltip2.set_fontsize(10)
        self._tooltip2.set_fontweight('bold')
        self._tooltip2.set_visible(True)

        self.draw()

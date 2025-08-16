"""visualization.py

提供圖表與互動元件：
- PieChartArea：可嵌入任意 Qt 版面的甜甜圈圖（內圈為估算發電量佔比，外圈顯示與實際差額），支援 auto/full/compact/mini 標籤模式與可選工具列。
- plot_tag_trends：在單圖中疊多條時間序列曲線。
- CustomToolbar：延伸 NavigationToolbar2QT 以修正儲存對話框行為。
- TrendWindow：簡易 QMainWindow 包裝 matplotlib Figure。
- TrendChartCanvas：支援滑鼠互動提示的 FigureCanvas。

此模組不處理商業邏輯；建議外部先計算，再用 PieChartArea.update_from_metrics() 餵資料重繪。
"""
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas, NavigationToolbar2QT
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from logging_utils import get_logger
import pandas as pd
from typing import List, Optional, Tuple, Dict, Iterable, Callable
from PyQt6 import QtWidgets, QtCore
from PyQt6.QtWidgets import QFileDialog

logger = get_logger(__name__)

class PieChartArea(QtCore.QObject):
    """嵌入在任意 QLayout/QWidget 的可重繪甜甜圈圖視圖。

    Parameters
    ----------
    host : QtWidgets.QLayout | QtWidgets.QWidget
        要放置圖表的容器；可直接給 layout，或給 widget（若 widget 無 layout 會自動補 QVBoxLayout）。
    with_toolbar : bool, default False
        是否加入 Matplotlib 工具列。
    dpi : int, default 120
        Figure dpi。
    figsize : Tuple[float, float], default (6.2, 6.2)
        Figure 初始尺寸（英吋）。實際顯示大小仍由 Qt 版面決定；此值影響文字擁擠程度。
    """

    def __init__(self,
                 host: object,
                 *,
                 with_toolbar: bool = False,
                 dpi: int = 120,
                 figsize: Tuple[float, float] = (6.2, 6.2)):
        super().__init__()

        # 1) 找到/建立 layout
        layout: Optional[QtWidgets.QLayout]
        if isinstance(host, QtWidgets.QLayout):
            layout = host
            self._host_widget = layout.parentWidget()
        elif isinstance(host, QtWidgets.QWidget):
            self._host_widget = host
            layout = host.layout()
            if layout is None:
                layout = QtWidgets.QVBoxLayout(host)
                host.setLayout(layout)
        else:
            raise TypeError("host 必須是 QLayout 或 QWidget")
        self._layout = layout

        # 2) 建立 Figure/Canvas（只建一次）
        self._fig, self._ax = plt.subplots(figsize=figsize, dpi=dpi)
        self._fig.set_constrained_layout(True)  # 減少文字被裁切

        self._canvas = FigureCanvas(self._fig)
        self._canvas.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )
        self._layout.addWidget(self._canvas)

        self._toolbar = None
        if with_toolbar:
            from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar
            self._toolbar = NavigationToolbar(self._canvas, self._host_widget)
            self._toolbar.setIconSize(QtCore.QSize(16, 16))
            self._layout.addWidget(self._toolbar)

        # 3) 外觀/行為設定（可由外部修改）
        self._colors: Dict[str, str] = {
            "NG":  "#F5A623",  # 橘
            "MG":  "#6BBF59",  # 綠
            "COG": "#4A90E2",  # 藍
        }
        self._order: Tuple[str, str, str] = ("NG", "MG", "COG")
        self._label_mode: str = "auto"  # 'auto' | 'full' | 'compact' | 'mini'
        self._show_diff_ring: bool = True
        self._title: Optional[str] = "TGs 燃氣→發電量估算（內圈）與實際差額（外圈）"

    # -------------------- 可調屬性 API --------------------
    def set_colors(self, colors: Dict[str, str]) -> None:
        """覆寫 NG/MG/COG 顏色。例如 {'NG':'#...', 'MG':'#...', 'COG':'#...'}"""
        if colors:
            self._colors.update(colors)

    def set_order(self, order: Iterable[str]) -> None:
        """設定顯示順序，例如 ("NG","COG","MG")。"""
        self._order = tuple(order)

    def set_label_mode(self, mode: str) -> None:
        """'auto' | 'full' | 'compact' | 'mini'。"""
        if mode not in {"auto", "full", "compact", "mini"}:
            raise ValueError("label_mode 僅支援 'auto'|'full'|'compact'|'mini'")
        self._label_mode = mode

    def set_show_diff_ring(self, enabled: bool) -> None:
        self._show_diff_ring = bool(enabled)

    def set_title(self, title: Optional[str]) -> None:
        self._title = title

    # -------------------- 主要繪圖 API（建議用這個） --------------------
    def update_from_metrics(
        self,
        *,
        flows: Dict[str, float],      # {'NG': Nm3/h, 'COG': Nm3/h, 'MG': Nm3/h}
        est_power: Dict[str, float],  # {'NG': MW,   'COG': MW,   'MG': MW}
        real_total: float,            # 實際總發電量 MW
        order: Optional[Iterable[str]] = None,
        colors: Optional[Dict[str, str]] = None,
        show_diff_ring: Optional[bool] = None,
        title: Optional[str] = None,
    ) -> None:
        """用已計算好的數據重畫圖表（視圖不做商業計算）。
        請在**主執行緒**呼叫；若從 QThread 取得資料，請透過 signal 切回主執行緒。
        """
        if colors:
            self.set_colors(colors)
        if order:
            self.set_order(order)
        if show_diff_ring is not None:
            self.set_show_diff_ring(show_diff_ring)
        if title is not None:
            self.set_title(title)

        # 匯總
        est_total = float(sum(est_power.values()))
        gap = float(real_total - est_total)
        matched = max(0.0, min(real_total, est_total))

        # 無資料時的替代顯示
        if est_total <= 1e-9 and real_total <= 1e-9:
            self._ax.clear()
            self._ax.text(0.5, 0.5, "目前無可用的燃氣與發電資料",
                          ha="center", va="center", fontsize=11, transform=self._ax.transAxes)
            self._ax.axis("off")
            self._canvas.draw_idle()
            return

        # 依容器大小選擇標籤模式
        label_mode = self._decide_label_mode()

        # 內圈：估算占比
        self._ax.clear()
        vals = [float(est_power.get(k, 0.0)) for k in self._order]
        facecolors = [self._colors.get(k, "#999999") for k in self._order]

        def _fmt_flow(n: float) -> str:
            try:
                return f"{int(round(n)):,}"
            except Exception:
                return "0"

        if label_mode == "mini":
            labels = None
        elif label_mode == "full":
            labels = tuple(
                f"{k} {est_power.get(k,0.0):.2f} MW {_fmt_flow(flows.get(k,0.0))} Nm³/h"
                for k in self._order
            )
        else:  # compact
            labels = tuple(
                f"{k}{est_power.get(k,0.0):.2f} MW" for k in self._order
            )

        if label_mode == "mini":
            self._ax.pie(
                vals,
                labels=None,
                startangle=90,
                counterclock=False,
                wedgeprops=dict(width=0.45, edgecolor="white"),
                colors=facecolors,
            )
            # mini 模式用 legend 顯示資訊
            handles = []
            for k in self._order:
                handles.append(
                    Line2D([0], [0], lw=10, color=self._colors.get(k, "#999"),
                           label=f"{k}  {est_power.get(k,0.0):.2f} MW  |  {_fmt_flow(flows.get(k,0.0))} Nm³/h")
                )
            self._ax.legend(handles=handles,
                            loc="lower center", bbox_to_anchor=(0.5, -0.08),
                            ncol=1, frameon=False, fontsize=9)
        else:
            self._ax.pie(
                vals,
                labels=labels,
                labeldistance=1.06 if label_mode == "full" else 1.02,
                startangle=90,
                counterclock=False,
                wedgeprops=dict(width=0.45, edgecolor="white"),
                textprops=dict(fontsize=9 if label_mode == "full" else 8),
                colors=facecolors,
            )

        # 外圈：吻合 + 差額
        if self._show_diff_ring:
            gap_abs = abs(gap)
            ring_vals = [matched, gap_abs] if matched + gap_abs > 0 else [1, 0]
            ring_colors = ["#C0C0C0", ("#2ECC71" if gap >= 0 else "#E74C3C")]
            self._ax.pie(
                ring_vals,
                radius=1.0,
                startangle=90,
                counterclock=False,
                wedgeprops=dict(width=0.12, edgecolor="white"),
                colors=ring_colors,
                labels=None,
            )

        # 中央摘要
        gap_sign = "＋" if gap >= 0 else "－"
        gap_rate = (gap / real_total * 100.0) if real_total > 1e-9 else 0.0
        self._ax.text(
            0, 0,
            f"估算：{est_total:.2f} MW 實際：{real_total:.2f} MW 誤差：{gap_sign}{abs(gap):.2f} MW ({gap_rate:.1f}%)",
            ha="center", va="center", fontsize=10, fontweight="bold",
        )

        # 標題 + 等比例
        if self._title:
            self._ax.set_title(self._title, fontsize=11, pad=16)
        self._ax.axis("equal")

        self._fig.canvas.draw_idle()

    # --------------------（可選）相容舊用法：直接吃 Series 自行計算 --------------------
    def update(self,
               value: pd.Series,
               *,
               costs: Optional[Dict[str, float]] = None,
               get_costs_fn: Optional[Callable[[object], Dict[str, float]]] = None,
               unit_prices: Optional[object] = None,
               order: Optional[Iterable[str]] = None,
               colors: Optional[Dict[str, str]] = None,
               show_diff_ring: Optional[bool] = None,
               title: Optional[str] = None) -> None:
        """
        相容舊版：在視圖內部自行計算後再畫圖。
        不建議在高更新頻率下使用；較建議外部先算好再用 update_from_metrics()。
        """
        # 取得成本/熱值參數
        cal = self._resolve_costs(costs, get_costs_fn, unit_prices)

        # -> 計算 metrics
        flows, est_power, real_total = self._compute_metrics_from_series(value, cal)

        # -> 交給標準繪圖流程
        self.update_from_metrics(
            flows=flows,
            est_power=est_power,
            real_total=real_total,
            order=order,
            colors=colors,
            show_diff_ring=show_diff_ring,
            title=title,
        )

    # -------------------- Internals --------------------
    def _resolve_costs(self,
                       costs: Optional[Dict[str, float]],
                       get_costs_fn: Optional[Callable[[object], Dict[str, float]]],
                       unit_prices: Optional[object]) -> Dict[str, float]:
        if costs is not None:
            return dict(costs)
        if get_costs_fn is not None:
            return dict(get_costs_fn(unit_prices))
        raise ValueError("請提供 costs 或 get_costs_fn+unit_prices 以取得熱值/蒸氣轉換電力參數。")

    @staticmethod
    def _series_sum(series: pd.Series, idx_or_slice) -> float:
        try:
            sub = series.loc[idx_or_slice]
        except Exception:
            return 0.0
        if isinstance(sub, pd.Series):
            return float(pd.to_numeric(sub, errors="coerce").fillna(0).sum())
        return float(sub) if pd.notna(sub) else 0.0

    def _compute_metrics_from_series(self, value: pd.Series, cal: Dict[str, float]):
        # 1) 參數
        ng_heat  = float(cal.get("ng_heat", 0.0))
        cog_heat = float(cal.get("cog_heat", 0.0))
        ldg_heat = float(cal.get("ldg_heat", 0.0))
        bfg_heat = float(cal.get("bfg_heat", 0.0))
        steam_pw = float(cal.get("steam_power", 1.0)) or 1.0

        # 2) 動態混氣熱值
        bfg_total = self._series_sum(value, slice('BFG#1', 'BFG#2'))
        ldg_in    = self._series_sum(value, 'LDG Input')
        mg_in     = bfg_total + ldg_in
        mix_heat  = (bfg_total*bfg_heat + ldg_in*ldg_heat)/mg_in if mg_in>0 else 0.0

        # 3) 係數（Nm³/h -> MW）
        ng_factor  = ng_heat  / steam_pw / 1000.0
        cog_factor = cog_heat / steam_pw / 1000.0
        mg_factor  = mix_heat / steam_pw / 1000.0

        # 4) 流量合計
        flows = {
            "NG":  self._series_sum(value, slice('TG1 NG',  'TG4 NG')),
            "COG": self._series_sum(value, slice('TG1 COG', 'TG4 COG')),
            "MG":  self._series_sum(value, slice('TG1 Mix', 'TG4 Mix')),
        }

        # 5) 估算 MW
        est_power = {
            "NG":  max(0.0, flows["NG"]  * ng_factor),
            "COG": max(0.0, flows["COG"] * cog_factor),
            "MG":  max(0.0, flows["MG"]  * mg_factor),
        }

        # 6) 實際總 MW
        tg1_real = self._series_sum(value, slice('2H120','2H220'))
        tg2_real = self._series_sum(value, slice('5H120','5H220'))
        tg3_real = self._series_sum(value, slice('1H120','1H220'))
        tg4_real = self._series_sum(value, slice('1H320','1H420'))
        real_total = tg1_real + tg2_real + tg3_real + tg4_real

        return flows, est_power, real_total

    def _decide_label_mode(self) -> str:
        """依畫布像素尺寸與目前設定決定標籤模式。"""
        if self._label_mode != "auto":
            return self._label_mode
        w = max(1, self._canvas.width())
        h = max(1, self._canvas.height())
        m = min(w, h)
        # 閾值：<360 極小；<460 偏小；其他正常
        if m < 360:
            return "mini"
        if m < 460:
            return "compact"
        return "full"

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

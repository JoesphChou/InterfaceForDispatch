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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from logging_utils import get_logger
import pandas as pd
from typing import List, Optional, Tuple, Dict, Iterable
from PyQt6 import QtWidgets, QtCore
from PyQt6.QtWidgets import QFileDialog

logger = get_logger(__name__)

class PieChartArea(QtCore.QObject):
    """
    Mini-only 甜甜圈視圖（嵌入到 Qt Layout）。

    - 僅支援 'mini' 模式：以甜甜圈呈現。
    - 中央顯示：
        * show_diff_ring=True  → 三行（估算/實際/誤差）
        * show_diff_ring=False → 單行（實際發電量）
    - 扇區內顯示各燃氣之發電量：
        * show_diff_ring=True  → 推估發電量
        * show_diff_ring=False → 估算佔比 × 實際總發電量
    - 左下 legend 顯示：NG/COG/MG 目前流量/安全上限 Nm3/h (xx%)；僅列 flow>0。
    - 背景透明，與父層 widget 顏色一致。
    """

    def __init__(
        self,
        parent_layout: QtWidgets.QLayout,
        *,
        with_toolbar: bool = False,
    ) -> None:
        super().__init__()

        # 狀態
        self._order = ("NG", "COG", "MG")
        self._colors: Dict[str, str] = {
            "NG": "#1f77b4",
            "COG": "#ff7f0e",
            "MG": "#2ca02c",
        }
        self._show_diff_ring: bool = False
        self._title: Optional[str] = None
        self._mini_fontsize = 9
        self._center_font_sizes = (12, 11, 10, 9, 8, 7)
        self._donut_width = 0.45  # 甜甜圈寬度（同步用於內徑量測）

        # Matplotlib Figure / Canvas
        self._fig = Figure(figsize=(4.0, 3.2), dpi=100)
        self._ax = self._fig.add_subplot(111)
        self._canvas = FigureCanvas(self._fig)

        # 透明背景
        self._fig.patch.set_alpha(0.0)
        self._ax.set_facecolor('none')
        self._canvas.setStyleSheet("background: transparent;")

        # Qt 佈局安裝
        parent_layout.addWidget(self._canvas)
        if with_toolbar:
            from matplotlib.backends.backend_qt5 import NavigationToolbar2QT
            parent_layout.addWidget(NavigationToolbar2QT(self._canvas, parent_layout.parentWidget()))

    # ---------------------------- public setters ----------------------------
    def set_show_diff_ring(self, enabled: bool) -> None:
        self._show_diff_ring = bool(enabled)

    def set_colors(self, colors: Dict[str, str]) -> None:
        self._colors.update(colors or {})

    def set_order(self, order: Iterable[str]) -> None:
        self._order = tuple(order)

    def set_title(self, title: Optional[str]) -> None:
        self._title = title  # 不繪製，僅保留 API

    def set_mini_fontsize(self, pt: int) -> None:
        self._mini_fontsize = int(pt)

    def set_center_font_sizes(self, sizes: Iterable[int]) -> None:
        self._center_font_sizes = tuple(int(s) for s in sizes)

    # ------------------------------ helpers ------------------------------
    @staticmethod
    def _get_contrast_text_color(color) -> str:
        import matplotlib.colors as mcolors
        try:
            r, g, b, *_ = mcolors.to_rgba(color)
        except Exception:
            r, g, b = (0.5, 0.5, 0.5)
        def lin(c):
            return c/12.92 if c <= 0.03928 else ((c+0.055)/1.055)**2.4
        L = 0.2126*lin(r) + 0.7152*lin(g) + 0.0722*lin(b)
        cw = 1.05/(L+0.05)
        cb = (L+0.05)/0.05
        return "white" if cw > cb else "black"

    def _draw_in_wedge_labels(self, wedges, labels, *, donut_width: float, min_frac_inside: float = 0.06) -> None:
        """大扇區內標；小扇區自動移到外側，並加導線。"""
        import math
        if not labels:
            return

        r_mid = 1.0 - donut_width / 2.0
        self._fig.canvas.draw()

        for w, text in zip(wedges, labels):
            if not text:
                continue

            ang = math.radians((w.theta1 + w.theta2) / 2.0)
            frac = abs(w.theta2 - w.theta1) / 360.0

            if frac >= min_frac_inside:
                # 放扇區內
                x, y = r_mid * math.cos(ang), r_mid * math.sin(ang)
                tc = self._get_contrast_text_color(w.get_facecolor())
                self._ax.text(x, y, text, ha="center", va="center",
                              fontsize=8, color=tc, fontweight="bold")
            else:
                # 放扇區外 + 導線
                r_out = 1.0
                r_lab = 1.10
                x0, y0 = r_out * math.cos(ang), r_out * math.sin(ang)
                x1, y1 = r_lab * math.cos(ang), r_lab * math.sin(ang)
                ha = "left" if math.cos(ang) >= 0 else "right"
                x1 += 0.04 if ha == "left" else -0.04
                self._ax.annotate(
                    text,
                    xy=(x0, y0), xytext=(x1, y1),
                    ha=ha, va="center", fontsize=8, fontweight="bold",
                    arrowprops=dict(arrowstyle="-", lw=0.8, color="#666"),
                )

    def _build_mini_flow_legend(self, *, flows: Dict[str, float], tg_count: int) -> None:
        from matplotlib.patches import Rectangle
        per_tg_limit = {"COG": 24000, "MG": 200000, "NG": 10000}
        tg = tg_count if (tg_count and tg_count > 0) else 4
        handles, labels = [], []
        for k in ("NG", "COG", "MG"):
            if k not in self._order:
                continue
            f = float(flows.get(k, 0.0) or 0.0)
            if f <= 1e-9:
                continue
            limit = per_tg_limit.get(k, 0) * tg
            ratio = 0.0 if limit <= 0 else min(f/limit, 1.0)
            handles.append(Rectangle((0, 0), 1, 1, facecolor=self._colors.get(k, "#999"), edgecolor="none"))
            labels.append(f"{k}: {int(round(f)):,}/{limit:,} Nm3/h ({ratio*100:.0f}%)")
        if handles:
            leg = self._ax.legend(
                handles, labels,
                loc="lower left",
                bbox_to_anchor=(0.02, 0.02),
                bbox_transform=self._fig.transFigure,
                frameon=False,
                ncol=1,
                prop={"family": "monospace", "size": 8},
                handlelength=0.8, handletextpad=0.4, borderaxespad=0.0,
                columnspacing=0.4, labelspacing=0.2,
            )
            leg.set_in_layout(False)

    def _fit_center_text(self, text: str) -> None:
        donut_width = self._donut_width
        inner_r = 1.0 - donut_width
        self._fig.canvas.draw()
        renderer = self._fig.canvas.get_renderer()
        px_center = self._ax.transData.transform((0.0, 0.0))
        px_edge   = self._ax.transData.transform((inner_r, 0.0))
        px_radius = abs(px_edge[0] - px_center[0])
        px_diam   = 2.0 * px_radius
        t = self._ax.text(0, 0, text, ha="center", va="center", fontweight="bold")
        for fs in (*self._center_font_sizes, self._mini_fontsize, 7):
            t.set_fontsize(int(fs))
            self._fig.canvas.draw()
            bb = t.get_window_extent(renderer=renderer)
            if bb.width <= 0.92*px_diam and bb.height <= 0.92*px_diam:
                return
        t.set_fontsize(7)

    # ------------------------------ rendering ------------------------------
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
        tg_count: Optional[int] = None,
    ) -> None:
        if colors:
            self.set_colors(colors)
        if order:
            self.set_order(order)
        if show_diff_ring is not None:
            self.set_show_diff_ring(show_diff_ring)
        if title is not None:
            self.set_title(title)

        est_total = float(sum(est_power.values()))
        if not self._show_diff_ring:
            disp_power = {k: (float(est_power.get(k, 0.0))/est_total)*float(real_total) if est_total > 1e-9 else 0.0 for k in self._order}
        else:
            disp_power = {k: float(est_power.get(k, 0.0)) for k in self._order}

        self._ax.clear()
        vals = [float(disp_power.get(k, 0.0)) for k in self._order]
        facecolors = [self._colors.get(k, "#999999") for k in self._order]

        wedges, _ = self._ax.pie(
            vals,
            radius=1.0,
            labels=None,
            startangle=90,
            counterclock=False,
            wedgeprops=dict(width=self._donut_width, edgecolor="white"),
            colors=facecolors,
        )

        # 扇區內標註各燃氣發電量
        labels = []
        for k in self._order:
            v = float(disp_power.get(k, 0.0))
            labels.append(None if v <= 1e-9 else f"{k} : {v:.2f} MW")
        self._draw_in_wedge_labels(wedges, labels, donut_width=self._donut_width)

        # 外圈差額環（僅在 show_diff_ring=True）
        if self._show_diff_ring:
            gap = float(real_total - est_total)
            matched = max(0.0, min(real_total, est_total))
            gap_abs = abs(gap)
            ring_vals = [matched, gap_abs] if (matched + gap_abs) > 1e-9 else [1.0, 0.0]
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

        # 中央文字：依 show_diff_ring 顯示不同內容
        if self._show_diff_ring:
            gap = float(real_total - est_total)
            gap_sign = "＋" if gap >= 0 else "－"
            gap_rate = (gap/real_total*100.0) if real_total > 1e-9 else 0.0
            center_text = (
                f"估算：{est_total:.2f} MW\n"
                f"實際：{real_total:.2f} MW\n"
                f"誤差：{gap_sign}{abs(gap):.2f} MW ({gap_rate:.1f}%)"
            )
        else:
            center_text = f"發電量：{real_total:.2f} MW"
        self._fit_center_text(center_text)

        # 左下 legend：流量/上限
        self._build_mini_flow_legend(flows=flows, tg_count=tg_count or 4)

        self._ax.axis("equal")
        self._fig.canvas.draw_idle()


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

"""visualization.py

提供圖表與互動元件：
- PieChartArea：可嵌入任意 Qt 版面的甜甜圈圖（內圈為估算發電量佔比，外圈顯示與實際差額），支援 auto/full/compact/mini 標籤模式與可選工具列。
- plot_tag_trends：在單圖中疊多條時間序列曲線。
- CustomToolbar：延伸 NavigationToolbar2QT 以修正儲存對話框行為。
- TrendWindow：簡易 QMainWindow 包裝 matplotlib Figure。
- TrendChartCanvas：支援滑鼠互動提示的 FigureCanvas。

此模組不處理商業邏輯；建議外部先計算，再用 PieChartArea.update_from_metrics() 餵資料重繪。
"""
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5 import NavigationToolbar2QT
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
from logging_utils import get_logger
import pandas as pd
from typing import List, Optional, Tuple, Dict, Iterable, Callable
from PyQt6 import QtWidgets, QtCore
from PyQt6.QtWidgets import QFileDialog

logger = get_logger(__name__)

class PieChartArea(QtCore.QObject):
    """
    用於顯示燃氣發電比例的甜甜圈圖表。

    功能特色:
        - 以甜甜圈形式顯示，中央可切換顯示「實際/估算」發電量摘要。
        - 支援差額環 (show_diff_ring=True)：在甜甜圈外圈顯示估算與實際發電量的差異。
        - 可動態更新數據，包括各燃氣的流量、估算發電量與實際發電量。
        - 在燃氣總量為零或 TG 未運轉時，會自動顯示「未運轉 / 無資料」訊息。

    Attributes:
        _fig (matplotlib.figure.Figure): Matplotlib 圖表物件。
        _ax (matplotlib.axes.Axes): 主要繪圖座標軸。
        _show_diff_ring (bool): 是否啟用差額環顯示。
        _colors (Dict[str, str]): 各燃氣扇區顏色。
        _title (str): 圖表標題。

    常見使用情境:
        chart = PieChartArea(parent=some_layout)
        chart.update_from_metrics(
        ...     flows={"NG": 8000, "COG": 12000, "MG": 50000},
        ...     est_power={"NG": 50, "COG": 20, "MG": 10},
        ...     real_total=90.0,
        ...     tg_count=2,
        ...     show_diff_ring=True,
        ...     title="TG1 燃料發電比例"
        ... )
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

    # -----------------------------up- rendering ------------------------------
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
        """
        根據輸入的指標數據更新甜甜圈圖。

        Args:
            flows (Dict[str, float]): 各燃氣種類的流量 (Nm3/h)，例如 {"NG": 8000, "COG": 12000, "MG": 50000}。
            est_power (Dict[str, float]): 各燃氣估算發電量 (MW)，例如 {"NG": 50, "COG": 20, "MG": 10}。
            real_total (float): 實際總發電量 (MW)。
            order (Iterable[str], optional): 扇區順序，例如 ("NG","COG","MG")。
            colors (Dict[str, str], optional): 各燃氣的顏色設定。
            show_diff_ring (bool, optional): 是否顯示外圈差額環，預設沿用內部設定。
            title (str, optional): 圖表標題。
            tg_count (int, optional): TG 數量，用於計算流量上限比例。

        Notes:
            - 若估算與實際總發電量皆為零，將自動呼叫 render_inactive() 以顯示「未運轉 / 無資料」訊息。
            - 若 show_diff_ring=True，中央文字會同時顯示估算值、實際值與誤差。
        """

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

        # ---- 避免資料直接傳全0的資料時，呼叫繪圖而拋錯 ----
        total = sum(float(disp_power.get(k, 0.0)) for k in self._order)
        if total <= 1e-9:
            msg = "未運轉 / 無資料"
            self.render_inactive(title=title or self._title, message=msg)
            return

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

        if self._title:
            self._ax.set_title(self._title, fontsize=11, pad=16)

        self._ax.axis("equal")
        self._fig.canvas.draw_idle()

    def render_inactive(self, *, title: str = None, message: str = "未運轉 / 無資料") -> None:
        """
        在沒有有效數據時安全顯示「未運轉 / 無資料」狀態。

        Args:
            title (str, optional): 圖表標題，若提供則會更新標題。
            message (str, optional): 中央顯示的訊息，預設為「未運轉 / 無資料」。

        Notes:
            - 不繪製任何燃氣扇區，只顯示一個淡灰色甜甜圈底環。
            - 中央文字會自動縮放至適合甜甜圈內的大小。
            - 不會顯示 legend。
        """
        import matplotlib.patches as mpatches

        self._ax.clear()

        # 更新顏色/標題（若有）
        if title is not None:
            self._title = title  # 你若有上方 QLabel，可無視這行；保留不影響
        # 畫一個淡淡的甜甜圈底環當占位（可選）
        ring = mpatches.Wedge(center=(0, 0), r=1.0, theta1=0, theta2=360,
                              width=self._donut_width, facecolor="#e6e6e6", edgecolor="white", linewidth=1.0)
        self._ax.add_patch(ring)

        # 中央文字（自動縮字到甜甜圈內）
        self._fit_center_text(str(message))

        # 不顯示 legend
        leg = self._ax.get_legend()
        if leg is not None:
            leg.remove()

        if self._title:
            self._ax.set_title(self._title, fontsize=11, pad=16)
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
            self.figure.savefig(filename)

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

class StackedAreaCanvas(FigureCanvas):
    """
    互動式堆疊區域圖畫布（適用 PyQt6 / Matplotlib）。

    功能特性
    ----------
    - 支援多層 tooltip：每層各一個，底色 = 系列色、白字白框，避免與邊界衝突並盡量避免互相重疊。
    - 滑鼠移動顯示紅色垂直線與「時間徽章」（紅底白字），位置貼齊 x 軸刻度區。
    - 於各層上緣繪製對應顏色的圓點，並隨滑鼠移動更新。
    - 圖層順序可依平均值由小到大（由下而上）自動排序，或套用預設順序（by_unit: TRT/CDQ/TG；by_fuel: NG/COG/MG）。
    - 底部 legend 置於 x 軸下方，並可在滑鼠移動時即時更新每類別的數值與占比，以及「總計」。

    主要使用情境
    ------------
    以 30~120 分鐘的即時資料為 x 軸（DatetimeIndex），
    y 軸為發電量（MW）。常見欄位組合：
      - by_unit: ["TRT", "CDQ", "TG"]（或各站有資料者）
      - by_fuel: ["NG", "COG", "MG"]

    注意事項
    --------
    - df.index 必須是 `pandas.DatetimeIndex`；`df.columns` 為系列名稱。
    - 圖例（legend）與 tooltip 的內容會在 plot() 後由 _on_mouse_move() 動態更新。
    - 本類別為 FigureCanvas 子類別，可直接加到 Qt 的 layout；背景透明（可由外層容器決定底色）。
    """

    def __init__(self, parent=None):
        """
        建立堆疊圖畫布與互動元件。

        參數
        ----
        parent : Optional[QWidget]
            外層 Qt 容器。若提供，畫布背景會維持透明以貼合容器底色。

        初始化內容
        ----------
        - 建立 Figure 與單一 Axes，預設關閉 Figure/Axes 的不透明背景以利融入外層底色。
        - 初始化互動狀態（系列標籤、顏色、各層 y 值、累積 y、時間軸、總計、模式等）。
        - 準備互動圖形物件：紅色垂直線、時間徽章、各層圓點與多個 tooltip。
        - 準備滑鼠事件連線（會在 plot() 內確保正確綁定與重綁）。
        """
        fig = Figure(figsize=(8, 4), dpi=100)
        self.ax = fig.add_subplot(111)
        fig.patch.set_alpha(0.0)    # Figure 背景透明
        self.ax.patch.set_alpha(0.0)    # Axes 背景透明
        super().__init__(fig)
        self.setParent(parent)

        # 狀態
        self._labels = None            # type: Optional[List[str]]
        self._colors = None            # type: Optional[List[str]]
        self._ys = None                # type: Optional[List[np.ndarray]]
        self._y_cum = None             # type: Optional[np.ndarray]
        self._times = None             # type: Optional[np.ndarray]
        self._total = None             # type: Optional[np.ndarray]
        self._mode = "by_unit"

        # 互動物件
        self._vline = None             # 紅線
        self._time_badge = None        # 時間徽章
        self._dots = []                # 各層圓點
        self._tips = []                # 各層 tooltip（Annotation）

        # 事件 id
        self._move_cid = None
        fig = Figure(figsize=(8, 4), dpi=100, constrained_layout=False)  # 建議關閉 tight/constrained 混用
        self._pad = dict(left=0.08, right=0.98, top=0.94, bottom=0.32)  # 固定邊界（下方預留給 legend）
        fig.subplots_adjust(**self._pad)

    def plot(self,
             df: pd.DataFrame,
             mode: str = "by_unit",
             legend_title: str = "",
             colors: Optional[Dict[str, str]] = None,
             order_policy: str = "auto",  # "auto"(小→大) 或 "preset"
             tooltip_fmt: Optional[Callable] = None,
             show_total_line: bool = True) -> None:
        """
        繪製堆疊區域圖並建立互動元素（紅線、時間徽章、各層圓點、tooltip、底部 legend）。

        參數
        ----
        df : pandas.DataFrame
            來源資料，`index` 必須為 `DatetimeIndex`；每個 column 是一個系列（例如 "TRT"、"CDQ"、"TG" 或 "NG"、"COG"、"MG"）。
        mode : {"by_unit", "by_fuel"}, default "by_unit"
            呈現模式，影響預設排序與 tooltip/legend 的語意。
        legend_title : str, default ""
            legend 的標題文字（會顯示在底部 legend 上方；若不需要可傳空字串）。
        colors : Optional[Dict[str, str]]
            系列顏色對應（hex 或 Matplotlib 認可的顏色字串）。未提供者會有預設色盤。
        order_policy : {"auto", "preset"}, default "auto"
            - "auto": 依各系列平均值由小到大排序（由下而上），避免小系列被遮蔽。
            - "preset": 採用預設順序（by_unit: TRT, CDQ, TG / by_fuel: NG, COG, MG）。
        tooltip_fmt : Optional[Callable[[datetime, Dict[str, float], float], str]]
            自訂 tooltip 內容的格式化函式；若為 None 則使用內建格式（by_unit: "TGs 發電量: xx.x MW"；by_fuel: "NG: xx.x MW"）。
        show_total_line : bool, default True
            是否在堆疊上方疊加「總計」折線。

        行為
        ----
        1) 依 order_policy 重新排序欄位，並以 stackplot 繪製堆疊。
        2) 建立或更新紅色垂直線與時間徽章，位置貼齊 x 軸。
        3) 為每個系列建立上緣圓點與對應 tooltip（預設隱藏，滑鼠移動時顯示）。
        4) 在 x 軸下方建立兩行 legend（必要時自動拆行），滑鼠移動時即時更新數值與百分比。
        5) 緩存互動必需的陣列（各層 y、累積 y、時間陣列、總計等）供 _on_mouse_move() 使用。
        6) 重新綁定滑鼠事件（避免多次 plot() 後事件重複）。

        例外
        ----
        - 若 df 為空，會清除座標並顯示「無資料」。

        效能備註
        --------
        - _times 與其數值化結果會快取（`_times_num`）以加速 _on_mouse_move() 的索引查找。
        - legend 的 Text 節點會被快取於 `_legend_texts`，以便快速覆寫內容。
        """
        # 固定瞇界，避免每次重算導致第一次被裁、第二次縮
        self.figure.subplots_adjust(**self._pad)

        assert isinstance(df.index, pd.DatetimeIndex), "df.index 必須為 DatetimeIndex"
        df = df.sort_index().astype(float).fillna(0.0)
        self._mode = mode

        # 預設色盤（沿用你原本，避免顏色跑掉）
        default_colors = {
            "TRT": "#9C27B0", "CDQ": "#FFA000", "TGs": "#4FC3F7",
            "NG": "#4E79A7", "COG": "#F28E2B", "MG": "#59A14F",
            "總計": "#90CAF9",
        }
        colors = colors or {}

        # 欄位順序（沿用你的 preset / auto）
        labels = list(df.columns)
        preset_unit = ["TRT", "CDQ", "TGs"]
        preset_fuel = ["NG", "COG", "MG"]
        preset = preset_unit if mode == "by_unit" else preset_fuel
        labels = [c for c in preset if c in labels] + [c for c in labels if c not in preset]
        if order_policy == "auto":
            labels = list(df[labels].mean().sort_values().index)

        # 過濾整段皆為 0 的系列（對應你需求 5：不顯示）
        labels = [c for c in labels if df[c].max() > 0]
        if not labels:
            self.ax.clear()
            self.ax.text(0.5, 0.5, "本區間皆為 0", ha="center", va="center", transform=self.ax.transAxes)
            self.draw()
            return

        df = df[labels]
        x_times = df.index.to_pydatetime()
        x_num = mdates.date2num(x_times)
        y_stack = [df[c].to_numpy(dtype=float) for c in labels]
        y_arr = np.vstack(y_stack)  # (n_series, n_points)
        y_cum = np.cumsum(y_arr, axis=0)  # 累積
        total = y_arr.sum(axis=0)

        # 清圖
        self.ax.clear()

        # 畫堆疊
        facecolors = [colors.get(c, default_colors.get(c, "#6FB1FF")) for c in labels]
        self.ax.stackplot(x_num, *y_stack, colors=facecolors, alpha=0.9, linewidth=0)

        # 總計線
        if show_total_line:
            self.ax.plot(x_num, total, lw=1.2, color=default_colors["總計"], alpha=0.9, zorder=5, label="總計")

        # 軸樣式
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        self.ax.set_xlim(x_num.min(), x_num.max())
        self.ax.set_ylim(bottom=0, top=float(np.nanmax(total) * 1.1 if np.nanmax(total) > 0 else 1.0))
        self.ax.grid(True, axis="y", linestyle=":", alpha=0.35)
        self.ax.margins(x=0)

        # --- legend：兩行自動換行（第一行不溢出，多的分類放到第二行；總計在第二行） ---
        # 預留底部空間（兩行 legend）
        self.figure.subplots_adjust(bottom=0.32)

        # 先確保 renderer 可用
        self.draw()
        renderer = self.get_renderer()

        # 以外層容器寬度為基準（不要乘 DPR）
        host = self.parentWidget()
        if host is not None:
            avail_px_container = float(host.width())
            try:
                m = host.contentsMargins()
                avail_px_container -= float(m.left() + m.right())
            except Exception:
                pass
        else:
            avail_px_container = float(self.figure.canvas.get_width_height()[0])

        ax_bbox = self.ax.get_window_extent(renderer=renderer)
        avail_px_axes = float(ax_bbox.width)

        avail_px = min(avail_px_container, avail_px_axes) * 0.92  # ← 真正可用寬度（與 x 軸對齊的 legend 一致）

        # ---- 2) 以「名稱 + 佔位字串」估最壞寬度，先完成兩行換行（第一行至少 2 個）----
        def _text_px(txt: str) -> float:
            t = self.ax.text(0, 0, txt, transform=self.ax.transAxes, alpha=0.0)
            bb = t.get_window_extent(renderer=renderer)
            t.remove()
            return float(bb.width)

        HANDLE_PAD_PX = 40.0
        suffix_main = "：000.0 MW (99.9%)"
        suffix_total = "：000.0 MW"

        labels_main = labels[:]  # 你的分類順序（不含總計）
        handles_main = [Line2D([0], [0], color=facecolors[i], lw=8) for i in range(len(labels_main))]
        item_px = [_text_px(s + suffix_main) + HANDLE_PAD_PX for s in labels_main]

        row1_idx, used = [], 0.0
        for i, w in enumerate(item_px):
            # ✅ 第一行至少放 2 個
            if used + w <= avail_px or len(row1_idx) < 2:
                row1_idx.append(i);
                used += w
            else:
                break
        row2_idx = list(range(len(labels_main)))[len(row1_idx):]

        def _sum_w(idxs):
            return sum(item_px[i] for i in idxs)

        # 第一行嚴格校正（仍保底 2 個）
        while _sum_w(row1_idx) > avail_px and len(row1_idx) > 2:
            row2_idx.insert(0, row1_idx.pop())

        # 第二行加上「總計」的寬度
        total_w = _text_px("總計" + suffix_total) + HANDLE_PAD_PX if show_total_line else 0.0

        # 第二行若仍超界，從第一行尾端搬（保底 2 個）
        while (_sum_w(row2_idx) + total_w) > avail_px and len(row1_idx) > 2:
            row2_idx.insert(0, row1_idx.pop())

        # ---- 3) 生成兩行 legend（此時只完成“換行”，未縮字）----
        row1_labels = [labels_main[i] for i in row1_idx]
        row1_handles = [handles_main[i] for i in row1_idx]
        row2_labels = [labels_main[i] for i in row2_idx]
        row2_handles = [handles_main[i] for i in row2_idx]
        if show_total_line:
            row2_labels += ["總計"]
            row2_handles += [Line2D([0], [0], color=default_colors["總計"], lw=8)]

        leg_row1 = self.ax.legend(
            row1_handles, row1_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.18),
            bbox_transform=self.ax.transAxes,
            ncol=max(2, len(row1_labels)),  # 第一行至少 2 欄
            frameon=False
        )
        leg_row2 = None
        if row2_labels:
            leg_row2 = self.ax.legend(
                row2_handles, row2_labels,
                loc="upper center",
                bbox_to_anchor=(0.5, -0.29),
                bbox_transform=self.ax.transAxes,
                ncol=len(row2_labels),
                frameon=False
            )
            self.ax.add_artist(leg_row1)
            self.ax.add_artist(leg_row2)

        # 緩存 legend 物件與文字（給之後 on_mouse_move 更新）
        self._leg_row1 = leg_row1
        self._leg_row2 = leg_row2
        self._legend_texts = {lab: text for lab, text in zip(row1_labels, leg_row1.get_texts())}
        if leg_row2 is not None:
            self._legend_texts.update({lab: text for lab, text in zip(row2_labels, leg_row2.get_texts())})

        # ---- 4) 若“兩行換好”後仍超界 → 才縮字（一次收斂到不超界或到最小字體）----
        def _legend_width(leg) -> float:
            if leg is None: return 0.0
            self.draw()
            return float(leg.get_window_extent(renderer=self.get_renderer()).width)

        def _set_fs(leg, fs):
            if leg is None: return
            for t in leg.get_texts():
                t.set_fontsize(fs)

        # 取當前字體大小
        cur_fs = None
        if leg_row1 and leg_row1.get_texts():
            cur_fs = leg_row1.get_texts()[0].get_fontproperties().get_size_in_points()
        elif leg_row2 and leg_row2.get_texts():
            cur_fs = leg_row2.get_texts()[0].get_fontproperties().get_size_in_points()
        if cur_fs is None: cur_fs = 10.0
        min_fs = 7.0

        w1 = _legend_width(leg_row1)
        w2 = _legend_width(leg_row2)
        while (w1 > avail_px or w2 > avail_px) and cur_fs > min_fs:
            cur_fs -= 1.0
            _set_fs(leg_row1, cur_fs)
            _set_fs(leg_row2, cur_fs)
            w1 = _legend_width(leg_row1)
            w2 = _legend_width(leg_row2)

        # 緩存 legend Text（用 label 名稱做 key，_on_mouse_move() 會更新內容）
        self._legend_texts = {lab: text for lab, text in zip(row1_labels, leg_row1.get_texts())}
        if leg_row2 is not None:
            self._legend_texts.update({lab: text for lab, text in zip(row2_labels, leg_row2.get_texts())})

        # 儲存互動資料（後續 tooltip/legend 更新用）
        self._labels = labels
        self._colors = facecolors
        self._ys = y_stack
        self._y_cum = y_cum
        self._times = df.index.values.astype("datetime64[ns]")
        self._total = total.astype(float)

        # 建立紅線與時間徽章（徽章貼齊 x 軸，紅線覆蓋刻度）
        if self._vline is not None:
            try:
                self._vline.remove()
            except Exception:
                pass
        self._vline = self.ax.axvline(x=x_num[-1], color="#E53935", lw=2, zorder=10)

        if self._time_badge is not None:
            try:
                self._time_badge.remove()
            except Exception:
                pass
        self._time_badge = self.ax.annotate(
            "", xy=(x_num[-1], 0), xycoords="data", xytext=(0, -2), textcoords="offset points",
            bbox=dict(boxstyle="round,pad=0.35", fc="#E53935", ec="#E53935"),
            color="white", ha="center", va="top", zorder=30, annotation_clip=False
        )

        # 每層圓點與 tooltip
        self._dots = []
        self._tips = []
        for i, lab in enumerate(labels):
            dot, = self.ax.plot([x_num[-1]], [y_cum[i, -1]], marker='o', ms=6,
                                color=facecolors[i], zorder=25)
            self._dots.append(dot)
            ann = self.ax.annotate(
                "", xy=(x_num[-1], y_cum[i, -1]), xycoords="data",
                xytext=(8, 8), textcoords="offset points",
                bbox=dict(boxstyle="round,pad=0.35", fc=facecolors[i], ec="white", lw=1.25),
                color="white", ha="left", va="center", zorder=26
            )
            ann.set_visible(False)
            self._tips.append(ann)

        self.draw()

        # 重新綁滑鼠事件（每次重畫都要重新 connect，否則會沒反應）
        if getattr(self, "_move_cid", None) is not None:
            try:
                self.mpl_disconnect(self._move_cid)
            except Exception:
                pass
        self._move_cid = self.mpl_connect('motion_notify_event', self._on_mouse_move)

    def _build_bottom_legend(self):
        """
        在 x 軸下方建立兩行 legend 佈局，並將 legend 的文字物件快取到 `_legend_texts`。

        行為
        ----
        - 為每個系列建立顏色方塊（handle）與標籤。
        - 另外建立「總計」的虛擬 handle，用以顯示當前總量文字。
        - 兩行配置：第一行放主要分類，第二行可加上「總計」或放不下的分類。
        - 將 legend 放置於 x 軸下方（使用 bbox_to_anchor 與 axes 座標系）。
        - 把每個 legend 的 Text 節點以「標籤文字」為 key 緩存到 `_legend_texts`，之後 _on_mouse_move() 會即時覆寫。

        備註
        ----
        - 這裡只負責建立與快取，實際的數值/百分比更新由 _update_bottom_legend(idx) 在滑鼠事件中負責呼叫。
        """
        self._legend_texts = {}  # label -> Text instance
        handles, labels = [], []

        for i, lab in enumerate(self._labels):
            patch = mpatches.Patch(color=self._facecolors[i], label=lab)
            handles.append(patch)
            labels.append(lab)

        # 總計的假 handle（用細線色）
        total_patch = mpatches.Patch(color="#90CAF9", label="總計")
        handles.append(total_patch)
        labels.append("總計")

        leg = self.ax.legend(
            handles, labels,
            loc="lower center", bbox_to_anchor=(0.5, -0.02),
            ncol=min(len(labels), 4), frameon=False, handlelength=1.8, columnspacing=1.6
        )
        # 把每個 Text 存起來，方便更新
        for txt in leg.get_texts():
            self._legend_texts[txt.get_text()] = txt

    def _update_bottom_legend(self, idx: int):
        """
        依目前滑鼠對應的時間索引 `idx`，即時更新底部 legend 的顯示內容。

        參數
        ----
        idx : int
            目前對應到 self._times / self._ys 的時間點索引。

        行為
        ----
        - 逐一覆寫各分類的文字為「{label}：{value:.1f} MW ({share:.1f}%)」。
        - 覆寫「總計」為「總計：{total:.1f} MW」。
        - 若總計為 0 或資料缺失，百分比顯示為 0.0%。

        需求條件
        --------
        - 需先由 plot() 設定好 `self._ys`、`self._total`（或 `_total_series`）以及 `_legend_texts`。
        """
        # 依目前 idx 更新 legend 文字（當下值與百分比）
        total = float(self._total_series[idx]) if hasattr(self, "_total_series") else 0.0
        # 各層
        for i, lab in enumerate(self._labels):
            val = float(self._ys[i, idx])
            share = (val / total * 100.0) if total > 0 else 0.0
            text = f"{lab}：{val:.1f} MW ({share:.1f}%)"
            if lab in self._legend_texts:
                self._legend_texts[lab].set_text(text)
        # 總計
        if "總計" in self._legend_texts:
            self._legend_texts["總計"].set_text(f"總計：{total:.1f} MW")

    # ------------------------------------------------------------------
    def _on_mouse_move(self, event):
        """
        滑鼠移動事件：更新紅線、時間徽章、圓點與多層 tooltip，並即時刷新底部 legend。

        主要流程
        --------
        1) 檢查事件是否在本 Axes 內，且互動必要資料（_times/_ys/_y_cum）已備好。
        2) 將 `self._times`（DatetimeIndex）轉為 Matplotlib 數值座標（快取於 `_times_num`）。
        3) 以 event.xdata 在 _times_num 找到最接近的索引 `idx`，計算目前時間 `xi`。
        4) 將紅色垂直線移至 `xi`，並把時間徽章貼齊 x 軸（顯示 `%H:%M`）。
        5) 取出各層當下值與累積值（用於圓點與 tooltip 的 y 位置），
           - 若總量為 0：隱藏所有 tooltip 與圓點，直接重繪。
           - 否則依序顯示：
             * 以畫布寬度偵測 tooltip 是否會超出右界；若會，整批改靠左（及反之）。
             * 以像素距離控制多層 tooltip 的**最小垂直間距**，避免彼此重疊（逐層往上/往下微調位移）。
        6) 呼叫 _update_bottom_legend(idx) 以更新底部 legend 的文字。

        邊界與容錯
        ----------
        - 任何轉換或索引失敗時會採用保守 fallback（重新數值化時間或跳過該步）。
        - 對於空值或非數值資料，會以 0.0 代入避免例外。

        備註
        ----
        - 此事件連線於 plot() 內部確保每次重畫後只綁定一次（避免重複觸發）。
        """
        # 基本檢查
        if event.inaxes is not self.ax:
            return
        if self._times is None or self._y_cum is None or self._ys is None:
            return
        if event.xdata is None:
            return

        # 準備時間軸（數值化）
        # self._times 來自 plot(): df.index.values.astype("datetime64[ns]")
        try:
            # 盡量重用快取，沒有就現算
            x_num = getattr(self, "_times_num", None)
            if x_num is None or len(x_num) != len(self._times):
                x_dt = pd.to_datetime(self._times).to_pydatetime()
                x_num = mdates.date2num(x_dt)
                self._times_num = x_num
        except Exception:
            x_dt = pd.to_datetime(self._times).to_pydatetime()
            x_num = mdates.date2num(x_dt)
            self._times_num = x_num

        # 找到最接近的索引
        idx = int(np.clip(np.searchsorted(x_num, event.xdata), 1, len(x_num) - 1))
        xi = x_num[idx]

        # 畫紅線 + 時間徽章（貼齊 x 軸）
        if self._vline is not None:
            self._vline.set_xdata([xi])
        if self._time_badge is not None:
            t = mdates.num2date(xi)
            self._time_badge.xy = (xi, 0.0)  # 貼齊 x 軸
            self._time_badge.set_text(t.strftime("%H:%M"))

        # 取當下值
        vals = np.array([s[idx] for s in self._ys], dtype=float)  # 各層當下值
        totals = self._y_cum[:, idx].astype(float)  # 各層上緣 y（累積）
        total_sum = float(self._total[idx]) if hasattr(self, "_total") else float(totals[-1])

        # 若總計為 0，隱藏 tooltip & dots，直接重繪
        if not np.isfinite(total_sum) or total_sum <= 0:
            for ann in getattr(self, "_tips", []):
                ann.set_visible(False)
            for dot in getattr(self, "_dots", []):
                dot.set_visible(False)
            self.figure.canvas.draw_idle()
            return

        # 依中線決定 tooltip 全體左右（左半邊 → 放右；右半邊 → 放左）
        x_left, x_right = self.ax.get_xlim()
        center_x = 0.5 * (x_left + x_right)
        place_left = (event.xdata > center_x)  # 在右半邊 → 放左
        x_off = -8 if place_left else 8
        ha = "right" if place_left else "left"

        # 先全部顯示 / 設定基礎內容，並計算像素位置
        renderer = self.get_renderer()
        min_gap_px = 18.0  # 層與層之間的最小像素間距（可再加大）
        floor_pad_px = 4.0  # 距 x 軸最小像素距離
        ypix_list = []
        ydat_list = []

        # 只顯示有意義的層（整段都為 0 的分類隱藏）
        show_mask = []
        for i, lab in enumerate(self._labels):
            show = True
            try:
                if np.nanmax(self._ys[i]) <= 0:
                    show = False
            except Exception:
                pass
            show_mask.append(show)

        # 更新圓點與初步 tooltip 內容
        for i, lab in enumerate(self._labels):
            y_top = totals[i]
            # dots
            if i < len(self._dots):
                self._dots[i].set_visible(show_mask[i] and (y_top > 0))
                self._dots[i].set_data([xi], [y_top])

            # tooltips
            if i < len(self._tips):
                ann = self._tips[i]
                if show_mask[i] and (y_top > 0):
                    ann.set_visible(True)
                    ann.xy = (xi, y_top)
                    ann.set_ha(ha)
                    ann.set_position((x_off, 8))
                    ann.set_text(self._format_tip_text(lab, vals[i]))
                    # 記錄像素 y，稍後排不重疊
                    ypix = self.ax.transData.transform((xi, y_top))[1]
                    ypix_list.append(ypix)
                    ydat_list.append(y_top)
                else:
                    ann.set_visible(False)

        # 依像素往上排，避免重疊；同時不低於 x 軸
        if ypix_list:
            # 取得 x 軸像素 y
            axis_bottom_pix = self.ax.transData.transform((xi, 0.0))[1] + floor_pad_px
            # 由下而上處理（totals 本身就由下到上遞增）
            last_pix = axis_bottom_pix - min_gap_px
            for i, lab in enumerate(self._labels):
                if i >= len(self._tips):
                    continue
                ann = self._tips[i]
                if not ann.get_visible():
                    continue
                # 目前欲放置的像素 y
                cur_pix = self.ax.transData.transform((xi, totals[i]))[1]
                # 與上一個保持最小間距
                cur_pix = max(cur_pix, last_pix + min_gap_px)
                # 轉回資料座標 y
                y_new = self.ax.transData.inverted().transform((0.0, cur_pix))[1]
                ann.xy = (xi, y_new)
                last_pix = cur_pix

        # 更新底部 legend 的即時數值
        # 需要你在 plot() 畫 legend 後，把 Legend 的文字節點收在 self._legend_texts
        if hasattr(self, "_legend_texts") and isinstance(self._legend_texts, dict):
            # 分類（依現有 legend 文字鍵來更新）
            for i, lab in enumerate(self._labels):
                if lab in self._legend_texts:
                    if total_sum > 0:
                        pct = vals[i] / total_sum * 100.0
                    else:
                        pct = 0.0
                    self._legend_texts[lab].set_text(f"{lab}：{vals[i]:.1f} MW ({pct:.1f}%)")
            # 總計
            if "總計" in self._legend_texts:
                self._legend_texts["總計"].set_text(f"總計：{total_sum:.1f} MW")

        # 觸發重繪
        self.figure.canvas.draw_idle()
        QtCore.QTimer.singleShot(0, self._shrink_legends_to_fit)

    def _format_tip_text(self, col: str, value: float) -> str:
        """
        產生滑鼠移動時每一層 tooltip 的顯示文字。

        規則
        ----
        - 依情境（self._mode / self.mode）切換樣式：
          * by_unit：顯示「<機組> 發電量: xx.x MW」
            - TG 與 TGs 都視為「TG」
          * by_fuel：顯示「<燃料> 發電量: xx.x MW」
        - 數值轉型失敗時視為 0.0。

        參數
        ----
        col : str
            欲顯示的系列名稱（例如 "TGs"、"CDQ"、"TRT" 或 "NG"、"COG"、"MG"）。
        value : float
            該系列在目前時間點對應的數值（MW）。

        回傳
        ----
        str
            依規則格式化後的字串，結尾固定包含「MW」單位。
        """
        # 你的類別裡可能叫 self._mode（在 plot() 內設定），也可能叫 self.mode
        mode = getattr(self, "_mode", getattr(self, "mode", "by_unit"))

        try:
            v = float(value)
        except Exception:
            v = 0.0

        if mode == "by_unit":
            # 依你的標籤慣例，TGs/TG 都視為「TG」
            if col in ("TGs", "TG"):
                head = "TG 發電量"
            else:
                head = f"{col} 發電量"
            return f"{head}: {v:.1f} MW"

        # by_fuel
        return f"{col} 發電量: {v:.1f} MW"

    def _shrink_legends_to_fit(self, min_fs: float = 7.0, margin_ratio: float = 0.92) -> None:
        """
        將底部「兩行」 legend 的字體在當前可用寬度下自動縮小，避免超出容器寬度。

        作法
        ----
        1) 先以 renderer 量測：
           - 外層容器（parentWidget）的有效寬度（扣掉左右內容邊距）
           - 當前 Axes 的畫布寬度
           取兩者較小者再乘上 margin_ratio 當作可用寬度。
        2) 量測第一、二行 legend 目前字體大小下的實際像素寬度。
        3) 只要任一行超過可用寬度，就同步將兩行的字體大小以 1pt 逐步遞減，
           直到兩行都不再超出，或達到最小字體 min_fs 為止。
        4) 不處理版面配置（換行）邏輯；只負責字體縮放。

        參數
        ----
        min_fs : float, 預設 7.0
            允許縮小的最小字體大小（pt）。
        margin_ratio : float, 預設 0.92
            為避免緊貼邊界的保守係數，乘上後當作可用寬度。

        回傳
        ----
        None
        """
        # 沒 legend 就不用做
        leg1 = getattr(self, "_leg_row1", None)
        leg2 = getattr(self, "_leg_row2", None)
        if leg1 is None and leg2 is None:
            return

        # 量可用寬度：取 container 與 Axes 兩者較小
        self.draw()
        renderer = self.get_renderer()
        host = self.parentWidget()
        if host is not None:
            avail_px_container = float(host.width())
            try:
                m = host.contentsMargins()
                avail_px_container -= float(m.left() + m.right())
            except Exception:
                pass
        else:
            avail_px_container = float(self.figure.canvas.get_width_height()[0])

        ax_bbox = self.ax.get_window_extent(renderer=renderer)
        avail_px_axes = float(ax_bbox.width)
        avail_px = min(avail_px_container, avail_px_axes) * margin_ratio

        def _legend_width(leg):
            if leg is None:
                return 0.0
            self.draw()
            return float(leg.get_window_extent(renderer=self.get_renderer()).width)

        def _set_fs(leg, fs):
            if leg is None:
                return
            for t in leg.get_texts():
                t.set_fontsize(fs)

        # 取目前字體大小
        cur_fs = None
        if leg1 and leg1.get_texts():
            cur_fs = leg1.get_texts()[0].get_fontproperties().get_size_in_points()
        elif leg2 and leg2.get_texts():
            cur_fs = leg2.get_texts()[0].get_fontproperties().get_size_in_points()
        if cur_fs is None:
            cur_fs = 10.0  # fallback

        # 只要任一行超界就同步縮小兩行字體
        w1 = _legend_width(leg1)
        w2 = _legend_width(leg2)
        # print(f"[legend] avail={avail_px:.1f}, w1={w1:.1f}, w2={w2:.1f}, fs={cur_fs:.1f}")
        while (w1 > avail_px or w2 > avail_px) and cur_fs > min_fs:
            cur_fs -= 1.0
            _set_fs(leg1, cur_fs)
            _set_fs(leg2, cur_fs)
            w1 = _legend_width(leg1)
            w2 = _legend_width(leg2)
            # print(f"[legend] shrink -> fs={cur_fs:.1f}, w1={w1:.1f}, w2={w2:.1f}")
        self.figure.canvas.draw_idle()

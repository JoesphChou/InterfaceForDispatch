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
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import numpy as np
import pandas as pd
import math
from logging_utils import get_logger
from typing import List, Optional, Tuple, Dict, Iterable, Callable
from PyQt6 import QtWidgets, QtCore
from PyQt6.QtWidgets import QFileDialog

logger = get_logger(__name__)

class GanttCanvas(FigureCanvas):
    """
    以甘特圖方式呈現製程排程。
    列：EAF / LF1-1 / LF1-2（EAF 內含 EAFA/EAFB）/ LF1 / LF2
    色：依類別與狀態（past/current/future）著色，方塊文字顯示爐號。
    """
    def __init__(self, *, row_order=("EAF", "LF1-1", "LF1-2", "LF1", "LF2")):
        self.fig = Figure(figsize=(7.5, 3.8), dpi=100)
        super().__init__(self.fig)
        self.ax = self.fig.add_subplot(111)
        self.row_order = list(row_order)
        self.row_height = 0.5       # 單列 bar 高度
        self.y_margin = 0.05        # Y 邊距比例
        self._time_line = None
        self._time_label= None

        # 透明背景
        self.fig.patch.set_alpha(0.0)   # 讓圖表外框透明
        self.ax.set_facecolor('none')   # 座標區透明
        self.setStyleSheet("background: transparent;")  # Qt canvas 背景透明

        # 顏色配置（可依喜好微調）
        self.proc_colors = {
            "EAF":  "#4E79A7",
            "LF1-1":"#F28E2B",
            "LF1-2":"#59A14F",
            "LF1":  "#FF62CD",
            "LF2":  "#4DB5B2",
        }
        self.state_alpha = {
            "past":   0.35,
            "current":0.95,
            "future": 0.65,
        }
        self.text_color = "#111"

        self.ax.grid(True, axis="x", linestyle=":", linewidth=0.8, alpha=0.5)
        self.ax.set_yticks(range(len(self.row_order)))
        self.ax.set_yticklabels(self.row_order)
        self.ax.invert_yaxis()  # 上面顯示 EAF

        # 時間軸格式
        self.ax.xaxis.set_major_locator(mdates.HourLocator())
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        self.fig.autofmt_xdate()

        self._bars = []  # 存每個 bar 與其附帶資訊，供 hover 判定
        # 預先建一個 annotation 做為 hover tip
        self._annot = self.ax.annotate(
            "", xy=(0, 0), xytext=(12, 14),
            textcoords="offset points",
            bbox=dict(boxstyle="round", fc="#222", ec="#ddd", alpha=0.85),
            fontsize=9, color="white"
        )
        self._annot.set_visible(False)
        self._annot.set_zorder(99)  # 將hover tip 暫定保持在最上層
        # 綁定滑鼠事件
        self.mpl_connect("motion_notify_event", self._on_hover)
        self._layer_artists=[]  # 存上一次畫的 bar/文字等，供下次移除

    def _row_index(self, proc: str) -> int:
        # EAFA/EAFB 都歸到 EAF 這列
        key = "EAF" if proc in ("EAFA", "EAFB") else proc
        if key not in self.row_order:
            self.row_order.append(key)
            self.ax.set_yticks(range(len(self.row_order)))
            self.ax.set_yticklabels(self.row_order)
        return self.row_order.index(key)

    def _bars_from_df(self, df, state: str):
        """
        以 df["開始時間"]/df["結束時間"] 為座標畫 bar，並把 hover 需要的欄位塞進 self._bars。
        注意：呼叫 plot() 前已將不同 phase 的資料轉成「g_start/g_end → 開始/結束」。
        """
        if df is None or df.empty:
            return

        for _, r in df.iterrows():
            start = mdates.date2num(pd.to_datetime(r["開始時間"]))
            end = mdates.date2num(pd.to_datetime(r["結束時間"]))
            width = max(end - start, 1 / 1440)
            raw_proc = r.get("製程")
            proc = "EAF" if raw_proc in ("EAFA", "EAFB") else raw_proc
            y = self._row_index(raw_proc)
            color = self.proc_colors.get(proc, "#888")

            rect = self.ax.barh(
                y=y, width=width, left=start, height=self.row_height,
                color=color, alpha=self.state_alpha.get(state, 0.8),
                edgecolor="white", linewidth=1.0, zorder=3
            )[0]

            # 收集 hover 用 metadata
            furnace = str(r.get("爐號", "")) or ""
            # 將「狀態」對齊原本程式用的 key（之前寫的是 "製程狀態"）
            status = (str(r.get("製程狀態", "")) or str(r.get("狀態", "")) or "").strip()
            status_end = r.get("狀態結束")

            self._bars.append({
                "patch": rect,
                "proc": proc,  # "EAF" / "LF1-1" / "LF1-2" / "LF1" / "LF2"
                "raw_proc": raw_proc,  # 保留 EAFA/EAFB 以利判斷
                "furnace": furnace,
                "start": pd.to_datetime(r.get("表定開始時間") or r["開始時間"]),
                "end": pd.to_datetime(r.get("表定結束時間") or r["結束時間"]),
                "state": state,
                "status": status,
                "status_end": status_end,
                "actual_start": r.get("實際開始時間"),
                "actual_end": r.get("實際結束時間"),
            })

            # 爐號小標
            if furnace:
                x_center = start + width / 2.0
                txt = self.ax.text(x_center, y, furnace, ha="center", va="center",
                                   fontsize=9, color=self.text_color, zorder=5)
                self._layer_artists.append(txt)

            self._layer_artists.append(rect)

    def _format_tip(self, info: dict) -> str:
        """
        規則：
          - EAF 需顯示 (A爐)/(B爐)
          - Past:  第2行顯示「表定」，第3行顯示「實際」
          - Current:
              * 顯示「表定」
              * 若有 status_end（狀態結束） → 額外加「預計HH:MM結束」
              * 僅 EAF/LF1-1/LF1-2 顯示「狀態：xxx」
          - Future: 顯示「表定」
        """
        proc = info['proc']  # "EAF" / "LF1-1" / "LF1-2" / ...
        raw_proc = info.get('raw_proc', "")  # EAFA / EAFB / ...
        furnace = info.get('furnace', "") or ""
        state = info.get('state', "")
        status = (info.get('status') or "").strip()
        status_end = info.get('status_end')

        start = info['start']  # 表定開始（已在 _bars_from_df 塞好）
        end = info['end']  # 表定結束
        a_st = info.get('actual_start')
        a_ed = info.get('actual_end')

        def hhmm(ts):
            try:
                return pd.Timestamp(ts).strftime("%H:%M")
            except Exception:
                return "--:--"

        # 第一行：EAF 顯示 A/B 爐
        if proc == "EAF":
            suffix = " (A爐)" if str(raw_proc) == "EAFA" or furnace.upper().startswith("A") else \
                " (B爐)" if str(raw_proc) == "EAFB" or furnace.upper().startswith("B") else ""
            first = f"EAF{suffix} {furnace}".strip()
        else:
            first = f"{proc} {furnace}".strip()

        lines = [first]

        # 第二行：表定
        lines.append(f"表定：{hhmm(start)} ~ {hhmm(end)}")

        # 第三行起：依 phase
        if state == "past":
            # Past 顯示實際
            if a_st is not None and a_ed is not None:
                lines.append(f"實際：{hhmm(a_st)} ~ {hhmm(a_ed)}")

        elif state == "current":
            # 有狀態結束 → 預計 xx:xx 結束
            if status_end is not None and pd.notna(status_end):
                lines.append(f"預計{hhmm(status_end)}結束")
            # 僅 EAF / LF1-1 / LF1-2 顯示製程狀態
            if proc in ("EAF", "LF1-1", "LF1-2") and status:
                lines.append(f"製程狀態：{status}")

        # Future 不再額外加行

        return "\n".join([t for t in lines if t])

    def _update_annot(self, patch, info):
        # 將註解框移到滑鼠附近；以 bar 中心點當 anchor
        x = patch.get_x() + patch.get_width()/2.0
        y = patch.get_y() + patch.get_height()/2.0
        self._annot.xy = (x, y)
        self._annot.set_text(self._format_tip(info))
        self._annot.get_bbox_patch().set_alpha(0.98)

    def _on_hover(self, event):
        vis = self._annot.get_visible()
        if event.inaxes != self.ax:
            if vis:
                self._annot.set_visible(False)
                self.draw_idle()
            return

        # 找出滑鼠所在的 bar
        hit_any = False
        for info in self._bars:
            patch = info["patch"]
            contains, _ = patch.contains(event)
            if contains:
                self._update_annot(patch, info)
                self._annot.set_visible(True)
                self.draw_idle()
                hit_any = True
                break

        if not hit_any and vis:
            self._annot.set_visible(False)
            self.draw_idle()

    def _apply_style(self):
        # —— 背景：與周邊一致（透明）——
        try:
            self.fig.patch.set_alpha(0.0)  # Figure 透明
            self.ax.set_facecolor('none')  # 座標區透明
            self.setStyleSheet("background: transparent;")  # Qt Canvas 透明
        except Exception:
            pass

        # —— 網格與軸刻度格式（你要的樣式）——
        self.ax.grid(True, axis="x", linestyle=":", linewidth=0.8, alpha=0.5)
        self.ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        for tl in self.ax.get_xticklabels():
            tl.set_rotation(0)  # 水平

    def _clear_layers(self):
        """移除上一張圖所有動態圖層（bar/文字/輔助線/時間徽章…），避免重繪殘留。"""
        # 1) 先移除以往收集的動態 artist（bar、文字標籤等）
        if hasattr(self, "_layer_artists") and self._layer_artists:
            for art in self._layer_artists:
                try:
                    art.remove()
                except Exception:
                    pass
            self._layer_artists = []

        # 2) 清空 hover 需要的 bars 資訊
        self._bars = [] if hasattr(self, "_bars") else []

        # 3) 專門處理「現在時間」的垂直線與時間徽章
        if hasattr(self, "_time_line") and self._time_line is not None:
            try:
                self._time_line.remove()
            except Exception:
                pass
            self._time_line = None

        if hasattr(self, "_time_label") and self._time_label is not None:
            try:
                self._time_label.remove()
            except Exception:
                pass
            self._time_label = None

        # 4) 不要用 ax.cla()，以免把座標軸/格式全清掉；只移除我們管理的圖層

    def plot(self, past_df, current_df, future_df):
        """
        依 phase 轉換時間欄位後畫圖：
          - Past:    用 實際開始/實際結束 畫 bar（若缺值則跳過）
          - Current: 用 表定開始/表定結束 畫 bar；EAF/LF1-1/LF1-2 若「狀態結束」有值，終點覆蓋為 狀態結束
          - Future:  用 表定開始/表定結束 畫 bar
        並加入「現在時間」垂直虛線與 X 軸下方時間徽章（虛線 zorder 調低，不覆蓋 bar）。
        """
        # 僅清舊圖層
        self._clear_layers()
        self._bars = []

        # 1) 正規化列順序
        pref = ["EAF", "LF1-1", "LF1-2", "LF1", "LF2"]
        uniq = []
        for k in self.row_order:
            kk = "EAF" if k in ("EAFA", "EAFB") else k
            if kk not in uniq:
                uniq.append(kk)
        self.row_order = [k for k in pref if k in uniq] + [k for k in uniq if k not in pref]

        # 2) y 軸
        self.ax.set_yticks(range(len(self.row_order)))
        self.ax.set_yticklabels(self.row_order)
        n_rows = len(self.row_order)
        self.ax.set_ylim(-0.5, n_rows - 0.5)
        self.ax.invert_yaxis()

        def _prep(df, phase: str):
            if df is None or df.empty:
                return df
            x = df.copy()

            # 統一補上「製程狀態」欄位，來源為「狀態」
            if "製程狀態" not in x.columns and "狀態" in x.columns:
                x["製程狀態"] = x["狀態"]

            if phase == "past":
                x["g_start"] = x.get("實際開始時間")
                x["g_end"] = x.get("實際結束時間")
            elif phase == "current":
                x["g_start"] = x.get("表定開始時間", x.get("開始時間"))
                # 終點預設表定；EAF/LF1-1/LF1-2 若狀態結束非 NaT，覆蓋
                x["g_end"] = x.get("表定結束時間", x.get("結束時間"))
                mask = x["製程"].isin(["EAFA", "EAFB", "LF1-1", "LF1-2"]) & x.get("狀態結束").notna()
                x.loc[mask, "g_end"] = x.loc[mask, "狀態結束"]
            else:  # future
                x["g_start"] = x.get("表定開始時間", x.get("開始時間"))
                x["g_end"] = x.get("表定結束時間", x.get("結束時間"))

            # 丟掉缺值
            x = x[(x["g_start"].notna()) & (x["g_end"].notna())].copy()

            # 讓 _bars_from_df 畫圖時吃到「開始時間/結束時間」與 hover 顯示需要的欄位
            x["開始時間"] = x["g_start"]
            x["結束時間"] = x["g_end"]
            return x

        past_v = _prep(past_df, "past")
        current_v = _prep(current_df, "current")
        future_v = _prep(future_df, "future")

        # 3) 畫圖（past -> future -> current 疊層）
        self._bars_from_df(past_v, "past")
        self._bars_from_df(future_v, "future")
        self._bars_from_df(current_v, "current")

        # 4) 自動 X 範圍 + 留白
        all_times = []
        for df in (past_v, current_v, future_v):
            if df is not None and not df.empty:
                all_times += list(pd.to_datetime(df["開始時間"]).values)
                all_times += list(pd.to_datetime(df["結束時間"]).values)
        if all_times:
            tmin = pd.to_datetime(min(all_times))
            tmax = pd.to_datetime(max(all_times))
            pad = pd.Timedelta(minutes=15)
            self.ax.set_xlim(mdates.date2num((tmin - pad).to_pydatetime()),
                             mdates.date2num((tmax + pad).to_pydatetime()))

        # 5) 現在時間：垂直虛線 + X 軸下方時間徽章
        now = pd.Timestamp.now()
        x_now = mdates.date2num(now.to_pydatetime())
        # 虛線（zorder 低於 bar，避免蓋住）
        self._time_line = self.ax.axvline(
            x=x_now, linestyle=(0, (4, 4)), linewidth=1.0, color="#555", alpha=0.7, zorder=1)

        # 時間徽章（貼齊 x 軸下方）
        self._time_label = self.ax.annotate(
            now.strftime("%H:%M"),
            xy=(x_now, 0), xycoords=("data", "axes fraction"),
            xytext=(0, -3), textcoords="offset points",
            ha="center", va="top",
            bbox=dict(boxstyle="round,pad=0.25", fc="black", ec="none", alpha=0.85),
            color="white", fontsize=9, zorder=10
        )

        # 6) ytick 字級微調
        y_fs = 10 if n_rows <= 4 else 9 if n_rows <= 6 else 8
        for tl in self.ax.get_yticklabels():
            tl.set_fontsize(y_fs)

        self.draw()

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

    def _draw_in_wedge_labels(
            self,
            wedges,
            labels,
            *,
            donut_width: float,
            min_frac_inside: float = 0.06,  # 建議區間 0.06 ~ 0.15
            force_outside: bool = False,
    ) -> None:
        """
        在扇區內/外繪製雙行標籤；小扇區自動外移並加導線。

        規則
        ----
        - 以 min_frac_inside 判斷扇區角度是否足夠容納文字：
            * 若 force_outside=False 且扇區角度比例 >= min_frac_inside → 放「扇內」；
            * 否則放「扇外」，在外側加一段導線並微移文字，減少與扇區的碰撞。
        - 扇內標籤：放在內外半徑的中線位置（r = 1 - donut_width/2），
          採粗體、白色描邊（path effects）強化在深色底上的可讀性。
        - 扇外標籤：依扇區中心角的左右半平面決定對齊方向（left/right），
          並略做水平位移，降低外標籤與扇區的重疊。

        Parameters
        ----------
        wedges : List[matplotlib.patches.Wedge]
            Axes.pie() 回傳的 wedge 物件列表。
        labels : List[str]
            與 wedge 一一對應的字串（可含換行）。
        donut_width : float
            甜甜圈寬度（0~1），用以計算扇內標籤的極座標半徑。
        min_frac_inside : float, optional
            扇內標籤的最小角度比例門檻，預設 0.06（約 21.6°）。
        force_outside : bool, optional
            若為 True，全部標籤強制置於扇外（除錯/試調時可用）。

        Returns
        -------
        None
            僅進行繪圖，無回傳值。
        """
        if not labels:
            return

        r_mid = 1.0 - donut_width / 2.0
        self._fig.canvas.draw()

        for w, text in zip(wedges, labels):
            if not text:
                continue

            ang = math.radians((w.theta1 + w.theta2) / 2.0)
            frac = abs(w.theta2 - w.theta1) / 360.0

            if (not force_outside) and (frac >= min_frac_inside):
                # 扇內（兩行），黑字 + 白外框
                x, y = r_mid * math.cos(ang), r_mid * math.sin(ang)
                self._ax.text(
                    x, y, text,
                    ha="center", va="center",
                    fontsize=8, fontweight="bold",
                    color="black", linespacing=1.05, zorder=6,
                    path_effects=[pe.withStroke(linewidth=1.2, foreground="white", alpha=0.95)],
                )
            else:
                # 放扇區外 + 導線（兩行字）
                r_out = 1.0
                r_lab = 1.12  # 稍微再外一點，降低交疊
                x0, y0 = r_out * math.cos(ang), r_out * math.sin(ang)
                x1, y1 = r_lab * math.cos(ang), r_lab * math.sin(ang)
                ha = "left" if math.cos(ang) >= 0 else "right"
                x1 += 0.06 if ha == "left" else -0.06
                self._ax.annotate(
                    text,
                    xy=(x0, y0), xytext=(x1, y1),
                    ha=ha, va="center", fontsize=8, fontweight="bold",
                    linespacing=1.05,
                    arrowprops=dict(arrowstyle="-", lw=0.9, color="#666"),
                )

    def _build_mini_flow_legend(self, *, flows: Dict[str, float], tg_count: int, anchor: str = "ll") -> None:
        """
        於圖角落建立小型流量圖例：「目前流量/安全上限 Nm³/h (xx%)」。

        邏輯
        ----
        - 以每台 TG 的安全上限：COG=24,000、MG=200,000、NG=10,000（Nm³/h），
          乘上 tg_count 推得當前上限，並以 flows[k]/limit 計算百分比。
        - 僅顯示流量大於 0 的燃氣鍵值；顏色沿用各扇區顏色。
        - anchor 控制角落位置："ll"=左下、"lr"=右下；標籤字型採等寬以利數字對齊。
        - 此 legend 透過 bbox_to_anchor 固定在圖視窗角落，與扇區標籤相互獨立。

        Parameters
        ----------
        flows : Dict[str, float]
            各燃氣流量（Nm³/h），為 0 的燃氣不顯示。
        tg_count : int
            目前運轉的 TG 台數，用於計算總安全上限。
        anchor : str, optional
            "ll" 或 "lr"；預設左下 ("ll")，可視扇區標籤分佈動態切換以減少重疊。
        """
        # from matplotlib.patches import Rectangle
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
            ratio = 0.0 if limit <= 0 else min(f / limit, 1.0)
            handles.append(mpatches.Rectangle((0, 0), 1, 1, facecolor=self._colors.get(k, "#999"), edgecolor="none"))
            labels.append(f"{k}: {int(round(f)):,}/{limit:,} Nm3/h ({ratio * 100:.0f}%)")

        if not handles:
            return

        # 動態角落：左下或右下
        if anchor == "lr":
            bta = (0.98, 0.02)  # 右下
            loc = "lower right"
        else:
            bta = (0.02, 0.02)  # 左下
            loc = "lower left"

        leg = self._ax.legend(
            handles, labels,
            loc=loc,
            bbox_to_anchor=bta,
            bbox_transform=self._fig.transFigure,
            frameon=False,
            ncol=1,
            prop={"family": "monospace", "size": 8},
            handlelength=0.8, handletextpad=0.4, borderaxespad=0.0,
            columnspacing=0.4, labelspacing=0.2,
        )
        leg.set_in_layout(False)

    def _fit_center_text(self, text: str) -> None:
        """
        將多行/可變長度的摘要文字自動縮放，確保其完整置於甜甜圈內徑範圍。

        原理
        ----
        - 以目前設定的甜甜圈寬度 _donut_width 推得內徑半徑，換算為像素直徑。
        - 由大到小嘗試一組字級 `_center_font_sizes`（最後退而求其次使用較小字級），
          每次重繪並量測文字外框，直到寬高同時小於內徑 92% 為止。
        - 成功後維持該字級；若仍無法滿足，使用保底小字級以避免溢出。

        Parameters
        ----------
        text : str
            要顯示於甜甜圈中央的（可能含換行符號的）文字。
        """
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
        t.set_fontsize(9)  # 調整中間文字的大小

    # -----------------------------up-rendering ------------------------------
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
        group_label: Optional[str] = None,
    ) -> None:
        """
        依輸入指標重繪甜甜圈圖（含差額環、中心摘要與小型流量圖例）。

        繪製邏輯（重點）
        -------------
        - 顏色與順序：若 `colors`/`order` 提供，會先更新內部狀態。
        - 估算與顯示值：
            * `show_diff_ring=True`：扇區採用 `est_power`（估算）直接繪製。
            * `show_diff_ring=False`：將「實際總發電量」依估算佔比回配至各燃氣
              （你目前版本採「NG 固定、COG/MG 依剩餘比例」的新版邏輯可在外部先算好再傳入）。
        - 空資料防護：當分母為 0 或總量近似 0 時，改呼叫 render_inactive() 顯示「未運轉 / 無資料」。
        - 標籤：大扇區內標，小扇區自動移到外側並加導線（由 _draw_in_wedge_labels() 決定）。
        - 中央摘要：
            * `show_diff_ring=True`：顯示「估算、實際、誤差」三行。
            * `show_diff_ring=False`：顯示群組名稱（或 `group_label`）＋第二行為實際 MW。
          摘要字級由 _fit_center_text() 自動縮放。
        - 左下角 legend：呼叫 _build_mini_flow_legend() 以「目前流量/安全上限 Nm³/h (xx%)」樣式，
          並依扇區方位自動選擇左下或右下角，盡量避開標籤重疊。

        Parameters
        ----------
        flows : Dict[str, float]
            各燃氣流量（Nm³/h），鍵通常為 "NG"/"COG"/"MG"。
        est_power : Dict[str, float]
            估算的各燃氣發電量（MW）。
        real_total : float
            實際總發電量（MW）。視 show_diff_ring 決定是否用於回配顯示。
        order : Iterable[str], optional
            扇區順序，預設使用內部順序（例如 ("NG","COG","MG")）。
        colors : Dict[str, str], optional
            各燃氣顏色表。
        show_diff_ring : bool, optional
            是否顯示差額外環（不提供則沿用現狀）。
        title : str, optional
            圖表標題；若外層以 QLabel 呈現標題，此參數可為 None。
        tg_count : int, optional
            目前運轉 TG 數量，供流量安全上限計算用。
        group_label : str, optional
            當 title 未使用時，可在中心摘要第一行顯示的群組/機組名稱（例如 "TG1"、"TGs"）。

        Notes
        -----
        - 僅處理視覺化與健壯性檢查，不進行商業邏輯計算；建議於外部先規則化資料再傳入。
        - 此函式需在 GUI 主執行緒執行（QThread 請透過 signal/slot 回到主執行緒）。
        """

        if colors:
            self.set_colors(colors)
        if order:
            self.set_order(order)
        if show_diff_ring is not None:
            self.set_show_diff_ring(show_diff_ring)
        # ----- 標題與上緣間距 -----
        if self._title:
            self._ax.set_title(self._title, fontsize=11, pad=14)
            # 有標題：留一點頭部空間
            try:
                self._fig.subplots_adjust(top=0.88)
            except Exception:
                pass
        else:
            # 無標題：不佔用上方空間
            self._ax.set_title("")  # 確保不殘留舊標題
            try:
                self._fig.subplots_adjust(top=0.97)
            except Exception:
                pass

        # --- 新版：show_diff_ring 決定扇區用電量的算法 ---
        #   False：NG 維持估算；COG/MG 以剩餘 (real_total - NG估算) 按占比縮放
        #   True ：維持原本顯示估算值
        est_total = float(sum(float(est_power.get(k, 0.0)) for k in self._order))
        ng_est = float(est_power.get("NG", 0.0))
        cog_est = float(est_power.get("COG", 0.0))
        mg_est = float(est_power.get("MG", 0.0))

        if not self._show_diff_ring:
            # NG 不變
            ng_disp = max(0.0, ng_est)

            # 剩餘要分配給 COG/MG 的實際電量
            remain_real = max(0.0, float(real_total) - ng_disp)

            # 兩者估算和
            rem_est_sum = cog_est + mg_est
            if rem_est_sum > 1e-9:
                cog_disp = max(0.0, remain_real * (cog_est / rem_est_sum))
                mg_disp = max(0.0, remain_real * (mg_est / rem_est_sum))
            else:
                cog_disp = 0.0
                mg_disp = 0.0

            # 依 self._order 輸出
            base = {"NG": ng_disp, "COG": cog_disp, "MG": mg_disp}
            disp_power = {k: float(base.get(k, 0.0)) for k in self._order}
        else:
            # 差額環模式：扇區顯示估算 MW
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

        # 扇區標籤（兩行：燃氣名 + 發電量）
        labels = []
        for k in self._order:
            v = float(disp_power.get(k, 0.0))
            labels.append(None if v <= 1e-9 else f"{k}\n{v:.2f} MW")

        # 盡量移到扇區外，降低重疊
        self._draw_in_wedge_labels(
            wedges, labels,
            donut_width=self._donut_width,
            min_frac_inside=0.1,  # 提高到 1.0，理論上全部都會走「外側」
            force_outside=False,
        )

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
        grp = (group_label or "").strip()
        if not grp:
            grp = "TGs"
        if title:
            head = str(title).strip().split()[0]
            if head.upper().startswith("TG"):
                grp = head
        if self._show_diff_ring:
            # 解析群組標籤：優先 group_label（若你已有）；否則從 title 首字抓 "TGx"；最後預設 "TGs"
            gap = float(real_total - est_total)
            gap_sign = "＋" if gap >= 0 else "－"
            gap_rate = (gap / real_total * 100.0) if real_total > 1e-9 else 0.0
            center_text = (
                f"{grp}\n"  # ★ 新增在最上面
                f"估算：{est_total:.2f} MW\n"
                f"實際：{real_total:.2f} MW\n"
                f"誤差：{gap_sign}{abs(gap):.2f} MW ({gap_rate:.1f}%)"
            )
        else:
            center_text = f"{grp} 發電量\n{real_total:.2f} MW"
        self._fit_center_text(center_text)

        # ---- 外標籤預估落點（用扇區中線角度），若左下太擠就把 legend 放到右下 ----
        mid_angles = [0.5 * (w.theta1 + w.theta2) for w in wedges]  # 角度 0~360，0 在 x+，逆時針

        def quadrant(angle_deg):
            a = (angle_deg % 360)
            # 我們關心下半部：左下 ~225±45、右下 ~315±45
            if 180 <= a < 270:  # 第三象限（左下）
                return "LL"
            if 270 <= a < 360:  # 第四象限（右下）
                return "LR"
            return "OTHER"

        ll_count = sum(1 for a in mid_angles if quadrant(a) == "LL")
        lr_count = sum(1 for a in mid_angles if quadrant(a) == "LR")
        anchor = "ll" if ll_count <= lr_count else "lr"

        self._build_mini_flow_legend(flows=flows, tg_count=tg_count or 4, anchor=anchor)

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

        Attributes:
            self._ys (np.ndarray): shape=(n_series, n_points)
            self._y_cum (np.ndarray): 同上，逐層累加結果
            self._total (np.ndarray): shape=(n_points,)，各時點總量
            self._legend_texts (Dict[str, matplotlib.text.Text]): 由 label 對應到底部 legend 的 Text 節點

        初始化內容
        ----------
        - 建立 Figure 與單一 Axes，預設關閉 Figure/Axes 的不透明背景以利融入外層底色。
        - 初始化互動狀態（系列標籤、顏色、各層 y 值、累積 y、時間軸、總計、模式等）。
        - 準備互動圖形物件：紅色垂直線、時間徽章、各層圓點與多個 tooltip。
        - 準備滑鼠事件連線（會在 plot() 內確保正確綁定與重綁）。
        """
        fig = Figure(figsize=(8, 4), dpi=100)
        self.ax = fig.add_subplot(111)
        super().__init__(fig)
        self.setParent(parent)

        # 透明背景
        fig.patch.set_alpha(0.0)        # 讓圖表外框透明
        self.ax.set_facecolor('none')   # 座標區透明
        self.setStyleSheet("background: transparent;")  # Qt canvas 背景透明

        # 狀態
        self._labels = None
        self._colors = None
        self._ys = None
        self._y_cum = None
        self._times = None
        self._total = None
        self._total_series = None
        self._mode = "by_unit"

        # 互動物件
        self._vline = None
        self._time_badge = None
        self._dots = []
        self._tips = []

        # 事件 id
        self._move_cid = None
        self._last_idx = -1

        # ✅ 固定邊界（下方給 legend）
        self._pad = dict(left=0.08, right=0.98, top=0.94, bottom=0.32)
        self.figure.subplots_adjust(**self._pad)

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
        x_num = mdates.date2num(df.index.to_pydatetime())
        y_stack = [df[c].to_numpy(dtype=float) for c in labels]
        y_arr = np.vstack([df[c].to_numpy(dtype=float) for c in labels])
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
        self.renderer = self.get_renderer()

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

        ax_bbox = self.ax.get_window_extent(renderer=self.renderer)
        avail_px_axes = float(ax_bbox.width)

        avail_px = min(avail_px_container, avail_px_axes) * 0.92  # ← 真正可用寬度（與 x 軸對齊的 legend 一致）

        # ---- 2) 以「名稱 + 佔位字串」估最壞寬度，先完成兩行換行（第一行至少 2 個）----
        def _text_px(txt: str) -> float:
            t = self.ax.text(0, 0, txt, transform=self.ax.transAxes, alpha=0.0)
            bb = t.get_window_extent(renderer=self.renderer)
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
                row1_idx.append(i)
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

        # 先重置，避免重畫殘留
        self._legend_texts ={}

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

        # 把原本在 _on_mouse_move() 結尾呼叫縮字體的功能改來這邊執行，降低計算量
        self._shrink_legends_to_fit()

        # 儲存互動資料（後續 tooltip/legend 更新用）
        self._labels = labels
        self._colors = facecolors
        self._ys = y_arr
        self._y_cum = y_cum
        self._total = total                 # 給 _on_mouse_move 用

        self._times = df.index.values.astype("datetime64[ns]")
        self._times_num = None
        self._last_idx = -1

        self._total_series = self._total

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
        try:
            total = float(self._total[idx])  # ← 改用 self._total
        except Exception:
            total = 0.0

        # 各層
        for i, lab in enumerate(self._labels):
            try:
                val = float(self._ys[i, idx])  # 2D ndarray：行=系列、列=時間
            except Exception:
                val = 0.0
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

        # 只在「索引變了」才更新圖 (節流)
        if idx == getattr(self, "_last_idx", -1):
            return
        self._last_idx = idx

        xi = x_num[idx]
        # 畫紅線 + 時間徽章（貼齊 x 軸）
        if self._vline is not None:
            self._vline.set_xdata([xi])
        if self._time_badge is not None:
            t = mdates.num2date(xi)
            self._time_badge.xy = (xi, 0.0)  # 貼齊 x 軸
            self._time_badge.set_text(t.strftime("%H:%M"))

        # 取當下值
        vals = self._ys[:, idx].astype(float)
        totals = self._y_cum[:, idx].astype(float)  # 各層上緣 y（累積）
        total_sum = float(self._total_series[idx])

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
                    ann.set_text(self._format_tip_text(lab, float(vals[i])))
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
        self._update_bottom_legend(idx)
        # 觸發重繪
        self.figure.canvas.draw_idle()

    def _format_tip_text(self, col: str, value: float) -> str:
        """
        產生滑鼠移動時每層 tooltip 的顯示文字。

        規則
        ----
        - 依 self._mode 切換樣式：
            * "by_unit": 顯示「<機組> 發電量: xx.x MW」，其中 "TGs"/"TG" 一律顯示為「TG」。
            * "by_fuel": 顯示「<燃料> 發電量: xx.x MW」。
        - 非數值輸入會以 0.0 代入，輸出結尾固定包含「MW」。

        參數
        ----
        col : str
            系列名稱（例如 "TGs"、"CDQ"、"TRT" 或 "NG"、"COG"、"MG"）。
        value : float
            該系列於目前時間點的數值（MW）。

        回傳
        ----
        str
            已依模式與單位格式化的字串。
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
        將「底部兩行 legend」的字體在**當前可用寬度**下自動縮小（不更動換行），避免超出容器寬度。

        作法
        ----
        1) 量測可用寬度：取外層容器（parentWidget）與當前 Axes 寬度的較小值，再乘上 margin_ratio。
        2) 量測第一、二行 legend 在目前字體大小下的像素寬度。
        3) 若任一行超寬，則同步把兩行字體大小每次遞減 1pt，直到兩行皆不超寬或達到 min_fs。
        4) 本函式只負責**縮字**，不處理**換行**；換行在 plot() 內用「名稱 + 佔位字串」的寬度預估完成。

        參數
        ----
        min_fs : float, 預設 7.0
            允許縮小的最小字體大小（pt）。
        margin_ratio : float, 預設 0.92
            安全邊界係數；避免 legend 文字貼齊容器邊界而視覺擁擠。

        備註
        ----
        - 建議只在 plot() 完成 legend 佈局後呼叫一次，以降低互動時的負擔。
        - 在 _on_mouse_move() **不應再呼叫**，避免造成效能下降。
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

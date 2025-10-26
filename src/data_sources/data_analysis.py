import pandas as pd
import numpy as np
from typing import Optional
from scipy.signal import find_peaks
import matplotlib.pyplot as plt

def estimate_speed_from_last_peaks(
    power: pd.Series,
    *,
    threshold: float,
    power_filter: pd.Series = None,
    smooth_window: int = 3,
    distance: int = 1,
    prominence: float = None,
    plot: bool = False
) -> dict:
    """
    根據最後兩個 peak 估算生產速率與每件 15 分鐘需量 (MW)，並額外回傳「目前生產速度」。

    參數：
      power : pd.Series，時間序列的功率（MW）
      threshold : float，檢測峰值的閾值
      power_filter : pd.Series，可選，扣除的雜訊
      smooth_window : int，平滑視窗大小
      distance : int，find_peaks 最小間隔（資料點數）
      prominence : float，可選，find_peaks 的 prominence
      plot : bool，是否繪圖顯示結果

    回傳 dict：
    {
        "production_normal": bool,         # 是否為正常生產（至少 2 個 peak）
        "peak_times": List[pd.Timestamp],  # 所有 peak 時間
        "delta_sec": float or None,        # A：最後兩 peak 間隔秒數
        "A_sec": float or None,            # 同 delta_sec（為除錯保留）
        "B_sec": float or None,            # B：最後一個 peak 距時間窗最右端的秒數
        "rate_items_per_15min": float,     # 「最後兩件」速率 (= 900/A)
        "current_rate_items_per_15min": float,  # 目前生產速度（依 A/B 規則）
        "mw_per_item": float or None       # 每件 15 分鐘需量 (MW)
    }
    """
    # 1) 扣除雜訊並平滑
    data = power.copy()
    if power_filter is not None:
        data = data - power_filter
    sm = data.rolling(smooth_window, center=True).mean().bfill().ffill()

    if prominence is None:
        prominence = (sm.max() - sm.min()) * 0.3

    # 2) 找 peaks
    peaks, _ = find_peaks(
        sm.values,
        height=threshold,
        prominence=prominence,
        distance=distance
    )
    peak_times = list(sm.index[peaks])

    right_edge = power.index[-1]  # 時間窗最右端
    production_normal = len(peak_times) >= 2

    result = {
        "production_normal": production_normal,
        "peak_times": peak_times,
        "delta_sec": None,
        "A_sec": None,
        "B_sec": None,
        "rate_items_per_15min": 0.0,
        "current_rate_items_per_15min": 0.0,
        "mw_per_item": None,
    }

    # === 先處理 A（最後兩峰間隔）與 rate_items_per_15min ===
    if production_normal:
        t1, t2 = peak_times[-2], peak_times[-1]
        A = (t2 - t1).total_seconds()
        result["delta_sec"] = A
        result["A_sec"] = A

        if A > 0:
            result["rate_items_per_15min"] = 900.0 / A

        # 計算每件 15 分鐘需量 (MW)
        segment = power.loc[t1:t2]
        t_h = (segment.index - t1).total_seconds() / 3600.0
        mwh = np.trapz(segment.values, x=t_h)
        result["mw_per_item"] = mwh * 4.0  # MWh×4 = 15 分鐘 MW

    # === 再依你定義的規則計算「目前生產速度」 ===
    if len(peak_times) >= 2:
        # 有至少兩個 peak：比較 B 與 A
        last_peak = peak_times[-1]
        B = (right_edge - last_peak).total_seconds()
        result["B_sec"] = B

        A = result["A_sec"] or 0.0

        if B <= A:
            # 同步用 A 換算
            result["current_rate_items_per_15min"] = 900.0 / A if A > 0 else 0.0
        else:
            # B > A：若 B > 420 秒 → 目前速率為 0；否則用 B 換算
            if B > 420:
                result["current_rate_items_per_15min"] = 0.0
            else:
                result["current_rate_items_per_15min"] = 900.0 / B if B > 0 else 0.0

    elif len(peak_times) == 1:
        # 只有 1 個 peak：用 B 換算
        last_peak = peak_times[-1]
        B = (right_edge - last_peak).total_seconds()
        result["B_sec"] = B
        result["current_rate_items_per_15min"] = 900.0 / B if B > 0 else 0.0

    else:
        # 沒有 peak：速率為 0
        result["current_rate_items_per_15min"] = 0.0

    # 3) 視覺化（與原本一致）
    if plot:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(power.index, power.values, label='原始功率')
        if power_filter is not None:
            ax.plot(power_filter.index, power_filter.values, '--', label='濾波（補償）')
        ax.plot(sm.index, sm.values, label='平滑功率')
        ax.axhline(threshold, color='r', linestyle='--', label=f'Th={threshold}')
        ax.plot(peak_times, sm.loc[peak_times], 'kx', label='所有峰值')
        if production_normal:
            ax.plot(
                [peak_times[-2], peak_times[-1]],
                [sm.loc[peak_times[-2]], sm.loc[peak_times[-1]]],
                'ro', markersize=8, label='最後兩峰'
            )
        ax.set_title('estimate_speed_from_last_peaks 檢視')
        ax.set_xlabel('時間')
        ax.set_ylabel('功率 (MW)')
        ax.legend()
        plt.tight_layout()
        plt.show()

    return result

def analyze_production_single_cycle(
    power: pd.Series,
    *,
    threshold: float,
    smooth_window: int = 3,
    distance: int = 1,
    prominence: Optional[float] = None,
    power_filter: Optional[pd.Series] = None,
    plot: bool = True
) -> dict:
    """
    方法一：以「生产1件平均时长」T1 为基准计算 unfinished。
    头/尾部 unfinished 仅在窗口首尾信号超过阈值时计算。

    Args:
        power (pd.Series):
            瞬時功率時間序列，索引為 datetime，值為功率 (MW)。

    Keyword Args:
        threshold (float):
            檢測閾值，超過此值視為生產狀態。
        smooth_window (int):
            平滑窗口大小（樣本數）。預設 3。
        distance (int):
            峰值檢測的最小樣本距離。預設 1。
        prominence (Optional[float]):
            峰值突顯度門檻；若 None 則使用 (sm.max() - sm.min()) * 0.3。預設 None。
        power_filter (Optional[pd.Series]):
            需扣除的干擾功率序列，索引需與 power 相同。預設 None。
        plot (bool):
            是否繪製分析圖。預設 True。

    Returns:
        dict: 包含以下欄位：
            method (str): 'single_cycle'，標識本方法。
            full_items (int): 完整週期內檢測到的件數。
            head_frac (float): 頭部未完成件比例。
            tail_frac (float): 尾部未完成件比例。
            total_items (float): 完整週期 + 未完成件數總和。
            rate_items_per_15min (float): 每 15 分鐘生產速率估算。
            total_kwh (float): 總耗電量（kWh）。
            kwh_per_item (Optional[float]): 每件平均耗電量（kWh），無耗電時為 None。
            demand_15m (float): 15 分鐘需量（MW）。
            cycles (List[Tuple[datetime, datetime, datetime]]): 完整週期的 (start, peak, end) 時間列表。
            T1_sec (Optional[float]): 平均完整週期時長（秒）。
            demand_per_item (Optional[float]): 每件單位需量（kWh）。
    """
    dt         = (power.index[1] - power.index[0]).total_seconds()
    start, end = power.index[0], power.index[-1]
    total_s    = (end - start).total_seconds() + dt

    data = power - power_filter if power_filter is not None else power.copy()
    sm = data.rolling(smooth_window, center=True).mean().bfill().ffill()

    if prominence is None:
        prominence = (sm.max() - sm.min()) * 0.3

    peaks, _   = find_peaks(sm, height=threshold, prominence=prominence, distance=distance)
    up_idx     = np.where((sm.values[:-1] < threshold) & (sm.values[1:] >= threshold))[0]
    down_idx   = np.where((sm.values[:-1] >= threshold) & (sm.values[1:] < threshold))[0]
    up_times   = sm.index[up_idx]
    down_times = sm.index[down_idx]
    peak_times = sm.index[peaks]

    # 配对完整周期
    cycles = []
    i = j = k = 0
    while i < len(up_times) and j < len(peak_times) and k < len(down_times):
        u, p, d = up_times[i], peak_times[j], down_times[k]
        if u < p < d:
            cycles.append((u, p, d))
            i += 1; j += 1; k += 1
        else:
            if p <= u:
                j += 1
            elif d <= p:
                k += 1
            else:
                i += 1

    full_cnt  = len(cycles)
    durations = [(d - u).total_seconds() for u, _, d in cycles]
    T1        = np.mean(durations) if durations else None

    head_frac = tail_frac = 0.0

    # 如果窗口开始时 signal ≥ threshold，就说明有一个未完成的“前半件”
    if full_cnt >= 1 and T1 and sm.iloc[0] >= threshold and down_times.size > 0:
        # 找到第一个 crossing-down (未配对的 d0)
        d0 = down_times[0]
        # 最近一个完整 cycle 的时长，用作归一化
        T_recent = (cycles[0][2] - cycles[0][0]).total_seconds()
        head_frac = min((d0 - start).total_seconds() / T_recent, 1.0)

    # 如果窗口结束时 signal ≥ threshold，就说明有一个未完成的“后半件”
    if full_cnt >= 1 and T1 and sm.iloc[-1] >= threshold and up_times.size > 0:
        # 找到最后一个 crossing-up (未配对的 u_last)
        u_last = up_times[-1]
        T_recent = (cycles[-1][2] - cycles[-1][0]).total_seconds()
        tail_frac = min((end - u_last).total_seconds() / T_recent, 1.0)

    if full_cnt >= 1 and T1:
        total_items = full_cnt + head_frac + tail_frac
    else:
        # fallback
        if not T1 or T1 == 0:
            total_items = 0.0
        else:
            duration_above = (sm >= threshold).sum() * dt
            if len(peaks) > 0:
                frac = np.clip(duration_above / T1, 0.5, 1.0)
            else:
                frac = 0.0
            total_items = frac

    # 速率：最后两件 peak-to-peak 间隔算速率；≤1 件时 total_items 即速率
    if full_cnt >= 2:
        interval = (cycles[-1][1] - cycles[-2][1]).total_seconds()
        rate_15m = 900.0 / interval if interval > 0 else 0.0
    else:
        rate_15m = total_items

    # 能量
    t_h          = (power.index - start).total_seconds() / 3600.0
    mwh          = np.trapz(power.values, x=t_h)
    total_kwh    = mwh * 1000.0
    demand_15m   = mwh * 4
    kwh_per_item = total_kwh / total_items if total_items > 0 else None
    demand_per_item = kwh_per_item * 4 / 1000.0 if total_items > 0 else None

    # 視覺化
    if plot:
        fig, axs = plt.subplots(3, 1, figsize=(10, 9), constrained_layout=True)

        # 原始 + 干擾
        axs[0].plot(power.index, power.values, label='原始電力')
        if power_filter is not None:
            axs[0].plot(power_filter.index, power_filter.values, '--', label='干擾')
        axs[0].set_title('1. 原始電力與干擾')
        axs[0].legend()

        # 平滑 + 事件
        axs[1].plot(sm.index, sm.values, label='平滑曲線')
        axs[1].hlines(
            y=threshold, xmin=start, xmax=end,
            colors='r', linestyles='--',
            label=f'Th={threshold}'
        )
        for u, p, d in cycles:
            axs[1].axvspan(u, d, color='green', alpha=0.3)
            axs[1].plot(p, sm[p], 'kx')
        axs[1].set_title('2. 平滑與生產週期檢測')
        axs[1].legend()

        # 摘要
        summary = (
            f"完整件: {full_cnt}\n"
            f"head_frac: {head_frac:.2f}, tail_frac: {tail_frac:.2f}\n"
            f"總件數: {total_items:.2f}\n"
            f"速率: {rate_15m:.2f} 件/15min\n"
            f"總耗電: {total_kwh:.1f} kWh\n"
            f"每件耗能: {kwh_per_item or 0:.2f} kWh\n"
            f"15min需量: {demand_15m:.2f} MW"
        )
        axs[2].axis('off')
        axs[2].text(0, 0.5, summary, fontsize=12)
        plt.show()

    return {
        "method": "single_cycle",
        "full_items": full_cnt,
        "head_frac": head_frac,
        "tail_frac": tail_frac,
        "total_items": total_items,
        "rate_items_per_15min": rate_15m,
        "total_kwh": total_kwh,
        "kwh_per_item": kwh_per_item,
        "demand_15m": demand_15m,
        "cycles": cycles,
        "T1_sec": T1,
        "demand_per_item": demand_per_item
    }

def analyze_production_avg_cycle(
    power: pd.Series,
    *,
    threshold: float,
    smooth_window: int = 3,
    distance: int = 1,
    prominence: Optional[float] = None,
    power_filter: Optional[pd.Series] = None,
    plot: bool = True
) -> dict:
    """
    方法二：以「生產1件+2件最近間隔時長」T2 為基準計算 unfinished，
    頭/尾部 unfinished 僅在窗口首尾信號超過閥值時計算。

        Args:
        power (pd.Series):
            瞬時功率時間序列，索引為 datetime，值為功率 (MW)。

    Keyword Args:
        threshold (float):
            檢測閾值，超過此值視為生產狀態。
        smooth_window (int):
            平滑窗口大小（樣本數）。預設 3。
        distance (int):
            峰值檢測的最<|...|>

    """
    dt         = (power.index[1] - power.index[0]).total_seconds()
    start, end = power.index[0], power.index[-1]
    total_s    = (end - start).total_seconds() + dt

    data = power - power_filter if power_filter is not None else power.copy()
    if smooth_window == 0:
        sm = data
    else:
        sm = data.rolling(smooth_window, center=True).mean().bfill().ffill()
    #sm   = data.rolling(smooth_window, center=True).mean().bfill().ffill()
    if prominence is None:
        prominence = (sm.max() - sm.min()) * 0.3

    peaks, _   = find_peaks(sm, height=threshold, prominence=prominence, distance=distance)
    up_idx     = np.where((sm.values[:-1] < threshold) & (sm.values[1:] >= threshold))[0]
    down_idx   = np.where((sm.values[:-1] >= threshold) & (sm.values[1:] < threshold))[0]
    up_times   = sm.index[up_idx]
    peak_times = sm.index[peaks]
    down_times = sm.index[down_idx]

    # 配对完整周期
    cycles = []
    i = j = k = 0
    while i < len(up_times) and j < len(peak_times) and k < len(down_times):
        u, p, d = up_times[i], peak_times[j], down_times[k]
        if u < p < d:
            cycles.append((u, p, d))
            i += 1; j += 1; k += 1
        else:
            if p <= u:
                j += 1
            elif d <= p:
                k += 1
            else:
                i += 1

    full_cnt = len(cycles)
    # 最近一次 up-to-up
    if full_cnt >= 2:
        T2_head = (cycles[1][0] - cycles[0][0]).total_seconds()
        T2_tail = (cycles[-1][0] - cycles[-2][0]).total_seconds()
    else:
        T2_head = T2_tail = None

    head_frac = tail_frac = 0.0

    if full_cnt >= 2 and T2_head and sm.iloc[0] >= threshold and down_times.size > 0:
        d0 = down_times[0]
        head_frac = min((d0 - start).total_seconds() / T2_head, 1.0)

    if full_cnt >= 2 and T2_tail and sm.iloc[-1] >= threshold and up_times.size > 0:
        u_last = up_times[-1]
        tail_frac = min((end - u_last).total_seconds() / T2_tail, 1.0)

    if full_cnt >= 2:
        total_items = full_cnt + head_frac + tail_frac
    else:
        if not T2_head or T2_head == 0:
            total_items = 0.0
        else:
            duration_above = (sm >= threshold).sum() * dt
            total_items = np.clip(duration_above / T2_head, 0.0, 1.0)

    # 速率
    if full_cnt >= 2:
        rate_15m = 900.0 / T2_tail if T2_tail and T2_tail > 0 else 0.0
    else:
        rate_15m = total_items

    # 能量
    t_h          = (power.index - start).total_seconds() / 3600.0
    mwh          = np.trapz(power.values, x=t_h)
    total_kwh    = mwh * 1000.0
    demand_15m   = mwh * 4
    kwh_per_item = total_kwh / total_items if total_items > 0 else None
    demand_per_item = kwh_per_item * 4 / 1000.0 if total_items > 0 else None

    # 視覺化
    if plot:
        fig, axs = plt.subplots(3, 1, figsize=(10, 9), constrained_layout=True)

        # 原始 + 干擾
        axs[0].plot(power.index, power.values, label='原始電力')
        if power_filter is not None:
            axs[0].plot(power_filter.index, power_filter.values, '--', label='干擾')
        axs[0].set_title('1. 原始電力與干擾')
        axs[0].legend()

        # 平滑 + 事件
        axs[1].plot(sm.index, sm.values, label='平滑曲線')
        axs[1].hlines(
            y=threshold, xmin=start, xmax=end,
            colors='r', linestyles='--',
            label=f'Th={threshold}'
        )
        for u, p, d in cycles:
            axs[1].axvspan(u, d, color='green', alpha=0.3)
            axs[1].plot(p, sm[p], 'kx')
        axs[1].set_title('2. 平滑與生產週期檢測')
        axs[1].legend()

        # 結果摘要
        summary = (
            f"完整件: {full_cnt}\n"
            f"head_frac: {head_frac:.2f}, tail_frac: {tail_frac:.2f}\n"
            f"總件數: {total_items:.2f}\n"
            f"速率: {rate_15m:.2f} 件/15min\n"
            f"總耗電: {total_kwh:.1f} kWh\n"
            f"每件耗能: {kwh_per_item or 0:.2f} kWh\n"
            f"15min需量: {demand_15m:.2f} MW"
        )
        axs[2].axis('off')
        axs[2].text(0, 0.5, summary, fontsize=12)
        plt.show()

    return {
        "method": "avg_cycle",
        "full_items": full_cnt,
        "head_frac": head_frac,
        "tail_frac": tail_frac,
        "total_items": total_items,
        "rate_items_per_15min": rate_15m,
        "total_kwh": total_kwh,
        "kwh_per_item": kwh_per_item,
        "demand_15m": demand_15m,
        "cycles": cycles,
        "T2_head_sec": T2_head,
        "T2_tail_sec": T2_tail,
        "demand_per_item": demand_per_item
    }


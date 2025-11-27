"""
mes_sample_tool.py

Offline-friendly snapshot_mes utilities for MES scraping flows.

本模組用於「爬 MES」時，將原始來源（HTML / JSON / DataFrame）快照存檔，並在離線環境中
以相同的資料重播（replay）。另提供情境化的 context manager，會臨時 monkey-patch
schedule_scraper 內部的 _fetch_soup(...) / _parse_2133_areas(...) 等讀取函式，
讓 scrape_schedule() 在離線下直接讀取你的快照。

Example
-------
>> # 1) 線上擷取（可在有網路的機器上進行一次）
>> import urllib.request as U
>> html_2138 = U.urlopen("http://example/2138.aspx", timeout=5).read().decode("utf-8","ignore")
>> from mes_sample_tool import save_mes_snapshot
>> save_mes_snapshot("2138", kind="html", content=html_2138, description="EAF chart page")

>> # 2) 離線重播：將 schedule_scraper 的讀取行為導向你的快照
>> from mes_sample_tool import use_mes_snapshots
>> with use_mes_snapshots({"2138": "snapshots/2138_*.html"}):
     # 執行你現有的解析流程（在離線環境下）
     from schedule_scraper import scrape_schedule
     res = scrape_schedule()
     print(res.ok, len(res.past), len(res.current), len(res.future))

設計說明
--------
- snapshot_mes 命名以 page 標記（'2138','2137','2133','2143'...）+ 時戳，利於管理
- 提供 save/load API；儲存格式：
    * html/json -> 純文字檔（UTF-8）
    * DataFrame -> CSV（含索引），同時產生 sidecar meta
- use_mes_snapshots(...) 以 unittest.mock.patch 在 with-block 期間替換抓取函式
"""

from __future__ import annotations
from datetime import datetime
from contextlib import contextmanager
from pathlib import Path
from typing import Mapping, Dict, Optional, Iterable, Union, Any
import logging
import json

import glob as _pyglob
from bs4 import BeautifulSoup
from unittest.mock import patch

logger = logging.getLogger(__name__)

# ---- snapshot_mes API -------------------------------------------------------
def _default_name(page: str, kind: str, base: Optional[str, Path] = None) -> Path:
    """
    Generate a proper snapshot_mes file path.

    改良版：
    - 若未給 base，預設放在 snapshots/<page>_<timestamp>.<ext>
    - 若 base 是資料夾，會自動在該資料夾內生成 <page>_<timestamp>.<ext>
    - 若 base 是檔名前綴（無副檔名），自動補上對應副檔名
    - 若 base 是完整檔名（含副檔名），照用不誤

    Parameters
    ----------
    page : str
        頁面代號（如 '2138'、'2137'）。
    kind : {'html','json','dataframe'}
        快照型態。
    base : str | Path | None
        可為 None、資料夾路徑、檔名前綴、或完整檔案路徑。

    Returns
    -------
    Path
        最終可直接寫入的檔案路徑。
    """
    from datetime import datetime
    from pathlib import Path

    ext_map = {"html": ".html", "json": ".json", "dataframe": ".csv"}
    ext = ext_map.get(kind, ".bin")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if base is None:
        # 沒指定路徑 -> 預設 snapshots/
        return Path("snapshots") / f"{page}_{ts}{ext}"

    p = Path(base)

    # 若是資料夾 -> 自動建立檔名
    if p.exists() and p.is_dir():
        return p / f"{page}_{ts}{ext}"

    # 若沒有副檔名 -> 補上
    if p.suffix == "":
        p = p.with_suffix(ext)

    return p

def save_mes_snapshot(
    page: str,
    *,
    kind: str,
    content: Any,
    path: Union[str, Path] | None = None,
    description: str = "",
    extra_meta: dict | None = None,
) -> Path:
    """Save a MES snapshot and its metadata to disk.

    This helper normalizes different kinds of MES source data (HTML/JSON/CSV)
    into files on disk and writes a companion ``*.meta.json`` that records
    how and when the snapshot was created. The combination of data file and
    metadata can later be used by offline tools to replay the same scenario.

    Args:
        page: MES page identifier, e.g. ``"2138"``, ``"2133"``, ``"2143"``,
            or ``"2137"``.
        kind: Content type: one of ``"html"``, ``"json"`` or ``"dataframe"``.
        content: The actual content to save. For ``"html"`` and ``"json"``,
            this is usually a string. For ``"dataframe"``, this must be
            a :class:`pandas.DataFrame`.
        path: Optional custom path or filename. If omitted, a default name
            is generated based on the page and current timestamp.
        description: Free-form description, such as the operating scenario
            or shift information, stored in the metadata for later recall.
        extra_meta: Optional dictionary of additional metadata fields to
            merge into the ``meta.json`` (for example, operator name or
            script version).

    Returns:
        Path: The actual path of the snapshot data file that was written.

    Raises:
        AssertionError: If ``kind == "dataframe"`` but ``content`` is not a
            :class:`pandas.DataFrame`.
        ValueError: If ``kind`` is not one of the supported values.
    """
    p = _default_name(page, kind, path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if kind == "html":
        p.write_text(str(content), encoding="utf-8")
    elif kind == "json":
        if isinstance(content, str):
            # 若已是 JSON 文字，仍原樣保存
            p.write_text(content, encoding="utf-8")
        else:
            p.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    elif kind == "dataframe":
        assert isinstance(content, pd.DataFrame), "content must be DataFrame when kind='dataframe'"
        content.to_csv(p, index=True)
    else:
        raise ValueError(f"Unsupported kind: {kind!r}")

    meta = dict(extra_meta or {})
    meta.update({
        "page": page,
        "kind": kind,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "description": description,
        "path": str(p),
    })
    meta_p = p.with_suffix(p.suffix + ".meta.json")
    meta_p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Saved MES snapshot_mes -> %s (meta: %s)", str(p), str(meta_p))
    return p

def load_mes_snapshot(path: Union[str, Path], kind: Optional[str] = None) -> Any:
    """Load a previously saved MES snapshot from disk.

    This function reads a snapshot file created by :func:`save_mes_snapshot`
    and returns its content in a convenient Python type. The file extension
    is used to infer the format (``.html``, ``.json`` or ``.csv``), but an
    optional ``kind`` argument can be supplied to enforce expectations.

    Args:
        path: Path to the snapshot file (HTML/JSON/CSV).
        kind: Optional expected content type, typically ``"dataframe"`` when
            loading a CSV snapshot. If provided and incompatible with the
            file extension, a :class:`ValueError` is raised.

    Returns:
        Any: The loaded content:
            * ``str`` for HTML/JSON snapshots.
            * :class:`pandas.DataFrame` for CSV snapshots.
            * Raw ``bytes`` as a fallback for other extensions.

    Raises:
        FileNotFoundError: If the given path does not exist.
        ValueError: If ``kind == "dataframe"`` but the file extension
            is not ``.csv``.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    suffix = p.suffix.lower()

    if kind and kind == "dataframe" and suffix != ".csv":
        raise ValueError("dataframe snapshot_mes must be a .csv file")

    if suffix == ".html":
        return p.read_text(encoding="utf-8")
    if suffix == ".json":
        return p.read_text(encoding="utf-8")
    if suffix == ".csv":
        return pd.read_csv(p, index_col=0, parse_dates=True)

    # fallback: raw bytes
    return p.read_bytes()

def _pick_latest(pattern: Union[str, Path]) -> Optional[Path]:
    """Resolve a pattern or directory into the most recent file.

    This utility supports multiple forms of input, all returning the
    "latest" file based on modification time:

    * A concrete file path (no wildcards) → returns the file itself
      if it exists.
    * A directory path (no wildcards) → returns the most recently
      modified file inside that directory.
    * A glob pattern with wildcards (``*``, ``?``, ``[]``) → evaluates
      the pattern via :mod:`glob` and returns the newest match.

    Args:
        pattern: File path, directory path or glob pattern to resolve.

    Returns:
        Optional[Path]: The selected file path, or ``None`` if nothing
        matching the pattern can be found.
    """
    s = str(pattern)

    # 是否包含萬用字元
    has_wild = any(ch in s for ch in "*?[]")

    if not has_wild:
        p = Path(s)
        if p.is_file():
            return p
        if p.is_dir():
            files = [f for f in p.iterdir() if f.is_file()]
            return max(files, key=lambda f: f.stat().st_mtime) if files else None
        # 不是檔也不是目錄 -> 視為不存在
        return None

    # 有萬用字元：改用 glob.glob（支援絕對與相對）
    candidates = [Path(x) for x in _pyglob.glob(s)]
    if not candidates:
        return None
    return max(candidates, key=lambda f: f.stat().st_mtime)

def _detect_page_token(url: str, keys: Iterable[str]) -> Optional[str]:
    """Detect which page token is contained in a URL.

    This helper scans the given URL string and returns the first key whose
    lowercase representation is found as a substring. It is used by
    :func:`use_mes_snapshots` to decide which snapshot should be used to
    fake a given HTTP request.

    Args:
        url: The original request URL (may be ``None`` or empty).
        keys: Iterable of candidate page identifiers, such as ``["2133",
            "2137", "2138", "2143"]``.

    Returns:
        Optional[str]: The first matching key, or ``None`` if no key
        is found in the URL.
    """
    if not url:
        return None
    u = url.lower()
    for k in keys:
        if str(k).lower() in u:
            return str(k)
    return None

@contextmanager
def use_mes_snapshots(
    mapping: Mapping[str, Union[str, Path]],
    *,
    schedule_module,
    encoding: str = "utf-8",
):
    """Temporarily redirect MES HTTP fetches to local HTML snapshots.

    This context manager monkey-patches the ``_fetch_soup`` function inside
    the given ``schedule_module`` so that any attempt to fetch MES pages
    (2133/2137/2138/2143) is served from local snapshot files instead of
    performing real network I/O.

    The mapping describes where each page's snapshot files live; the helper
    :func:`_pick_latest` is used to resolve patterns into actual files. When
    the context exits, the original ``_fetch_soup`` implementation is
    restored, even if an exception occurs inside the block.

    Args:
        mapping: Mapping from page identifiers (e.g. ``"2133"``) to either
            concrete paths, directory paths, or glob patterns where snapshot
            HTML files can be found.
        schedule_module: The imported module object that owns the original
            ``_fetch_soup`` function, typically
            ``src.data_sources.schedule_scraper``.
        encoding: Text encoding to use when reading snapshot files.

    Yields:
        None: This is a context manager; its only effect is the temporary
        patching of ``_fetch_soup`` during the ``with`` block.

    Raises:
        AttributeError: If the target module does not define ``_fetch_soup``.
        RuntimeError: If :class:`bs4.BeautifulSoup` is not available.
    """
    # 建立 routes
    routes: Dict[str, Optional[Path]] = {
        str(k): _pick_latest(v) for k, v in mapping.items()
    }

    ss = schedule_module  # 直接使用外部傳入的 module 實例

    _orig_fetch_soup = getattr(ss, "_fetch_soup", None)
    if _orig_fetch_soup is None:
        raise AttributeError("schedule_scraper._fetch_soup not found.")

    if BeautifulSoup is None:
        raise RuntimeError("BeautifulSoup is required but not installed.")

    keys = tuple(routes.keys())

    def _fake_fetch_soup(url: str, pool=None):
        token = _detect_page_token(url or "", keys)
        snap = routes.get(token) if token else None
        if snap and snap.exists():
            try:
                html = snap.read_text(encoding=encoding, errors="replace")
                return BeautifulSoup(html, "html.parser")
            except Exception as e:
                logger.warning("Failed to parse snapshot (%s), fallback to original", e)
        return _orig_fetch_soup(url, pool)

    patcher = patch.object(ss, "_fetch_soup", side_effect=_fake_fetch_soup)

    try:
        patcher.start()
        yield
    finally:
        try:
            patcher.stop()
        except Exception:
            pass

if __name__ == "__main__":
    """
    自我測試區（main execution block / self-test）

    用途：
    - 驗證 use_mes_snapshots() 是否正確攔截 _fetch_soup() 並改讀本地快照
    - 測試 2133 的解析流程（_parse_2133_areas），確保輸出結構與筆數合理

    專案預期結構（範例）：
    project_root/
    └─ src/
       ├─ data_source/
       │   └─ schedule_scraper.py
        ├─ mes/
        │   ├─ 2133_demo.html 或 2133_*.html
        │   └─ 2138_demo.html 或 2138_*.html
        └─ utils/
            └─ mes_sample_tool.py   ← 本檔
    """
    import sys
    from pathlib import Path
    import pandas as pd

    # 讓 Python 能 import 到 src/data_source/ 下的模組
    SRC = Path(__file__).resolve().parents[1]  # 指到 src/
    if str(SRC) not in sys.path:
        sys.path.append(str(SRC))

    # 指定快照位置（支援萬用字元；會自動挑選 mtime 最新檔）
    MES_DIR = SRC / "mes"
    mapping = {
        "2138": MES_DIR / "2138*.html",  # 例如：2133_demo.html 或 2133_20251108_1300.html
        "2137": MES_DIR / "2137*.html",  # 若要一併測 2138，可打開
        "2133": MES_DIR / "2133*.html",  # 若要一併測 2138，可打開
        "2143": MES_DIR / "2143*.html",  # 若要一併測 2138，可打開
    }

    print("[self-test] SRC      =", SRC.as_posix())
    print("[self-test] MES_DIR  =", MES_DIR.as_posix())
    print("[self-test] mapping  =", {k: str(v) for k, v in mapping.items()})

    try:
        with use_mes_snapshots(mapping):
            # 匯入 schedule_scraper（use_mes_snapshots 內部也會嘗試 data_source.schedule_scraper / schedule_scraper）
            try:
                from src.data_sources import schedule_scraper as ss
            except Exception:
                import src.data_sources.schedule_scraper as ss  # 備援匯入（若專案不是包成 data_source）
            now = pd.Timestamp("2025-11-10 13:33:46")
            result = ss.scrape_schedule(now=now)
            print(result)

    except Exception as e:
        # 讓錯誤清楚顯示，方便你定位（例如：匯入路徑、快照檔名、bs4 是否可用…）
        print("[self-test] ❌ Error during offline replay:", repr(e))
        raise

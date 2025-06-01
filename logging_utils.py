"""
logging_utils.py
通用日誌與錯誤處理工具，**Python 3.8 相容版**。

使用方法
--------
#>>> from logging_utils import setup_logging, get_logger, log_exceptions, timeit
#>>> setup_logging("logs/app.log", level="DEBUG")
#>>> logger = get_logger(__name__)

@log_exceptions()
@timeit()
def foo():
    logger.info("hello")

功能摘要
~~~~~~~~
1. 乾淨一致的 logging 設定（檔案 + console），支援旋轉、thread‑safe。
2. decorator / context‑manager 方便在各模組捕捉例外、量測執行時間。
3. 匯入時即有 console log，避免完全無輸出。
"""

from __future__ import annotations  # postpone evaluation of annotations

import functools
import logging
import time
import traceback
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Optional, Tuple, Union

__all__: Tuple[str, ...] = (
    "setup_logging",
    "get_logger",
    "timeit",
    "log_exceptions",
    "catch_and_log",
)

DEFAULT_LOG_FILE = "app.log"
DEFAULT_FORMAT = (
    "%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s"
)
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# 基本設定
# ---------------------------------------------------------------------------

def setup_logging(
    log_file: Union[str, Path] = DEFAULT_LOG_FILE,
    *,
    level: Union[int, str] = logging.INFO,
    console: bool = True,
    rotate: bool = True,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
    encoding: str = "utf-8",
) -> None:
    """一次性完成 logging 基本設定。

    若重複呼叫，僅在第一次真正生效。
    """

    root = logging.getLogger()
    if root.handlers:
        return  # 已設定過

    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    root.setLevel(level)

    formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DEFAULT_DATEFMT)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if rotate:
        fh: logging.Handler = RotatingFileHandler(
            log_path,
            mode="a",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=encoding,
        )
    else:
        fh = logging.FileHandler(log_path, encoding=encoding)

    fh.setFormatter(formatter)
    root.addHandler(fh)

    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        root.addHandler(ch)


# ---------------------------------------------------------------------------
# 便捷介面
# ---------------------------------------------------------------------------

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """取得（或建立）指定名稱的 logger。"""

    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Decorators / Context‑managers
# ---------------------------------------------------------------------------

def timeit(level: int = logging.DEBUG, logger: Optional[logging.Logger] = None):
    """函式計時 decorator。"""

    def decorator(func: Callable):
        log = logger or get_logger(func.__module__)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                log.log(level, "%s executed in %.2f ms", func.__qualname__, elapsed_ms)
        return wrapper

    return decorator


def log_exceptions(
    logger: Optional[logging.Logger] = None,
    *,
    reraise: bool = True,
    exc_types: Tuple[type, ...] = (Exception,),
):
    """捕捉例外並寫入日誌的 decorator。"""

    def decorator(func: Callable):
        log = logger or get_logger(func.__module__)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exc_types as e:  # noqa: B904
                log.error(
                    "Exception in %s: %s\n%s",
                    func.__qualname__,
                    e,
                    traceback.format_exc(),
                )
                if reraise:
                    raise
        return wrapper
    return decorator


@contextmanager
def catch_and_log(
    logger: Optional[logging.Logger] = None,
    *,
    reraise: bool = True,
    hint: str = "",
    exc_types: Tuple[type, ...] = (Exception,),
):
    """with 形態的錯誤攔截工具。"""

    log = logger or get_logger(__name__)
    try:
        yield
    except exc_types as e:  # noqa: B904
        log.error(
            "Exception%s: %s\n%s",
            f" ({hint})" if hint else "",
            e,
            traceback.format_exc(),
        )
        if reraise:
            raise


# ---------------------------------------------------------------------------
# 預設行為：import 時保底 Console log
# ---------------------------------------------------------------------------
# setup_logging(console=True, rotate=False)

# logging_utils.py 未尾不自動呼叫 setup_logging()
if __name__ == "__main__":  # pragma: no cover
    setup_logging(console=True, rotate=False)
    lg = get_logger("selftest")

    @log_exceptions()
    @timeit()
    def boom():
        lg.info("About to raise …")
        raise ValueError("demo")

    try:
        boom()
    except ValueError:
        lg.warning("ValueError captured as expected.")
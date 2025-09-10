from PyQt6 import QtCore, QtWidgets, QtGui
from typing import Sequence, Union, Optional
from logging_utils import get_logger
logger = get_logger(__name__)

Alignment = Union[str, QtCore.Qt.AlignmentFlag]
_ALIGN_MAP = {
    "left":   QtCore.Qt.AlignmentFlag.AlignLeft   | QtCore.Qt.AlignmentFlag.AlignVCenter,
    "center": QtCore.Qt.AlignmentFlag.AlignCenter,
    "right":  QtCore.Qt.AlignmentFlag.AlignRight  | QtCore.Qt.AlignmentFlag.AlignVCenter,
}

def _qbrush(color: Optional[str]) -> Optional[QtGui.QBrush]:
    if not color:
        return None
    return QtGui.QBrush(QtGui.QColor(color))

def _to_align(a: Alignment) -> QtCore.Qt.AlignmentFlag:
    if isinstance(a, QtCore.Qt.AlignmentFlag):
        return a
    return _ALIGN_MAP.get(str(a).lower(), QtCore.Qt.AlignmentFlag.AlignCenter)

def make_item(
    text_or_texts: Union[str, Sequence[str]],
    *,
    bold: bool = False,
    italic: bool = False,
    fg_color: Optional[str] = None,
    bg_color: Optional[str] = None,
    align: Union[Alignment, Sequence[Alignment]] = "center",
    font_size: int = 10,
):
    """
    多型 UI item 產生器：
      - 若傳入「字串」 -> 回傳 QTableWidgetItem（供 QTableWidget 使用）
      - 若傳入「list/tuple」 -> 回傳 QTreeWidgetItem（供 QTreeWidget 使用；每元素對應一欄）

    共同樣式：bold / italic / fg_color / bg_color / font_size
    對齊 (align)：
      - Table：單一值（'left'/'center'/'right' 或 Qt 對齊旗標）
      - Tree ：可為單一值或列表（逐欄對齊）
    """
    fg = _qbrush(fg_color)
    bg = _qbrush(bg_color)

    # ---- Tree: list/tuple -> QTreeWidgetItem ----
    if isinstance(text_or_texts, (list, tuple)):
        texts = ["" if t is None else str(t) for t in text_or_texts]
        item = QtWidgets.QTreeWidgetItem(texts)

        aligns = list(align) if isinstance(align, (list, tuple)) else [align] * len(texts)
        for col in range(len(texts)):
            f = QtGui.QFont()
            f.setPointSize(int(font_size))
            f.setBold(bool(bold))
            f.setItalic(bool(italic))
            item.setFont(col, f)
            if fg:
                item.setForeground(col, fg)
            if bg:
                item.setBackground(col, bg)
            item.setTextAlignment(col, _to_align(aligns[col] if col < len(aligns) else "center"))
        return item

    # ---- Table: str -> QTableWidgetItem ----
    text = "" if text_or_texts is None else str(text_or_texts)
    item = QtWidgets.QTableWidgetItem(text)
    f = item.font()
    f.setPointSize(int(font_size))
    f.setBold(bool(bold))
    f.setItalic(bool(italic))
    item.setFont(f)
    if fg:
        item.setForeground(fg)
    if bg:
        item.setBackground(bg)
    item.setTextAlignment(_to_align(align))
    return item
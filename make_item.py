from PyQt6 import QtCore, QtWidgets, QtGui
from logging_utils import get_logger, log_exceptions, timeit
logger = get_logger(__name__)

def make_item(text, bold=False, fg_color=None, bg_color=None, align='center', font_size=10):
    item = QtWidgets.QTableWidgetItem(text)
    alignment = {
        'left': QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignVCenter,
        'center': QtCore.Qt.AlignmentFlag.AlignCenter,
        'right': QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter
    }.get(align, QtCore.Qt.AlignmentFlag.AlignCenter)
    item.setTextAlignment(alignment)

    font = item.font()
    font.setPointSize(font_size)
    font.setBold(bold)
    item.setFont(font)

    if fg_color:
        item.setForeground(QtGui.QBrush(QtGui.QColor(fg_color)))
    if bg_color:
        item.setBackground(QtGui.QBrush(QtGui.QColor(bg_color)))

    return item
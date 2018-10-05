#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Author  : mofei
# @Time    : 2018/10/4 15:13
# @File    : hxqb.py
# @Software: PyCharm

import sys
import traceback

from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication

from widget import Widget

if __name__ == '__main__':
    try:
        app = QApplication(sys.argv)
        app.setWindowIcon(QIcon("papa.png"))

        w = Widget()
        w.show()

        sys.exit(app.exec_())
    except:
        print(traceback.format_exc())

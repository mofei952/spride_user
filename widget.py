#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Author  : mofei
# @Time    : 2018/10/4 15:26
# @File    : mwidget.py
# @Software: PyCharm

import sys
import datetime
import os
import re

from PyQt5 import QtCore
from PyQt5.QtCore import QCoreApplication
from PyQt5.QtWidgets import QWidget, QPushButton, QLineEdit, QLabel, QTextEdit, QHBoxLayout, QVBoxLayout, QMessageBox, \
    QFileDialog

import spride


class EmittingStream(QtCore.QObject):
    textWritten = QtCore.pyqtSignal(str)

    def write(self, text):
        self.textWritten.emit(str(text))


class Widget(QWidget):
    def __init__(self):
        super(Widget, self).__init__()

        sys.stdout = EmittingStream(textWritten=self.normalOutputWritten)
        sys.stder = EmittingStream(textWritten=self.normalOutputWritten)

        self.resize(800, 500)
        self.move(50, 50)
        self.setWindowTitle('爬虫小工具')

        self.urlLabel = QLabel('url')
        self.urlLineEdit = QLineEdit()
        self.urlLineEdit.setText('http://hxqb.xiazhuanke.com')

        self.cookieLabel = QLabel('cookie')
        self.cookielLineEdit = QLineEdit()
        # self.cookielLineEdit.setText('_9755xjdesxxd_=32; USER.ACCOUNT=aHhxYg==; gdxidpyhxdE=s%5Cz250mq0AaPwQDhH0qn7v5Tf41s7X2YJz7kokIL8B3HVtZmdb9efAJ4bN%5CDxdpd9Piq%5CEi2aO%2FfuAgT%2B3%2Fgvy2QV1SkRJfmV718ZRbiduDio7ylKa3HVkgx5ltKIVcVz7fblMXBO9Tt9KG0tSoVOKVLx9o%5CjmWSDVQPhG8ptzGucSyD%3A1538645096323; AUTH.YC.SBY=D165800876E25CE163D583DC44161F57AD41BEDF00296B4ECE99BBFFE12C3A877B017CEC4EBABAF8B8B9DA888650177059F0AED8C939BD27884E81707D206B25CB8296AAA3F132523EE0BD6C7408B85E771414F8D216E3E1D3FA97210DA0A3D940DB14F71A4774B983D2D4857FA0C5B5AFACBF7FC79B5ADC0799B98596ECBC61E22315EEB4E1FF7A6FCEEED65D6145989EC70D32C5D6F9DC1FD627BD72BEEF46F979EBA8EC3679601EA0C26BAF8426E96AA0B9878DC2376E5921B66DA6EAFD363DB6BC21853C10F831F579083397C06E924E6EB509C615C1E8751954FE4473993DD6FA2AA5B186584ED6263F4CA069FCA86B4E088AA6B72A30B9C17D556945D07912A15B8377E5FEC6CC1A7101A2FCA58B7FBD050827FFE9100DB99C0FA099B5C155678A77DBFA2D32F801900B6273B72A789C15384F45F0C9D9364FBFCB7164346609D4239F9CC8B1CED84A030740A10987C4768517A58E51C18ABC5B27A5F1174A4D727FEF7177974B679AF28CD57A3F0F431EC23ED80830DE046EC759C1566963FDD3E5EEBA79BA8C1D434F259296702F3CD5D3BE90593BF0DD926E41A8EED99201C79CA60ACF8E5944827F884A5674EF400E37E906502F16F307BB87B5296AC72EDCEC528DAFA98633FBC4AAD8BC307D241E15338693548C65824F4E4E824B68399569CF6F82F998D39C247FFB08CC95BDBFFE6EDEDD50C9E3BCF8F4B2F7FC95E655; AUTH.EXPIRED=MjAxODEwMDUwMTA4; SERVERID=aed0ea29d64e8dc43481ddfe295bddbb|1538644135|1538643896')

        self.startLabel = QLabel('开始月份')
        self.startLineEdit = QLineEdit()
        self.startLineEdit.setText('2017-11')

        self.endLabel = QLabel('结束月份')
        self.endLineEdit = QLineEdit()
        self.endLineEdit.setText('2018-01')

        self.saveLabel = QLabel('保存路径')
        self.saveLineEdit = QLineEdit()
        self.saveLineEdit = QLineEdit('')
        self.savePathButton = QPushButton('选择')

        self.urlIntervalLabel = QLabel('请求url的间隔时间')
        self.urlIntervalLineEdit = QLineEdit()
        self.urlIntervalLineEdit.setText('0.5')

        self.urlWaitLabel = QLabel('请求url的过于频繁的等待时间')
        self.urlWaitLineEdit = QLineEdit()
        self.urlWaitLineEdit.setText('5')

        self.calIntervalLabel = QLabel('请求通话记录的间隔时间')
        self.calIntervalLineEdit = QLineEdit()
        self.calIntervalLineEdit.setText('0.1')

        self.calErrorWaitLabel = QLabel('请求通话记录错误的等待时间')
        self.calErrorWaitLineEdit = QLineEdit()
        self.calErrorWaitLineEdit.setText('0')

        self.tellIntervalLabel = QLabel('请求通讯录的间隔时间')
        self.tellIntervalLineEdit = QLineEdit()
        self.tellIntervalLineEdit.setText('0.1')

        self.tellErrorWaitLabel = QLabel('请求通讯录错误的等待时间')
        self.tellErrorWaitLineEdit = QLineEdit()
        self.tellErrorWaitLineEdit.setText('0')

        self.calButton = QPushButton('通话记录', self)
        self.telButton = QPushButton('通讯录', self)
        self.allButton = QPushButton('全部', self)
        self.stopButton = QPushButton('停止', self)

        self.textEdit = QTextEdit()
        self.textEdit.setReadOnly(True)

        self.firstLayout = QHBoxLayout()
        self.firstLayout.addWidget(self.urlLabel)
        self.firstLayout.addWidget(self.urlLineEdit)
        self.firstLayout.addWidget(self.cookieLabel)
        self.firstLayout.addWidget(self.cookielLineEdit)

        self.secondLayout = QHBoxLayout()
        self.secondLayout.addWidget(self.startLabel)
        self.secondLayout.addWidget(self.startLineEdit)
        self.secondLayout.addWidget(self.endLabel)
        self.secondLayout.addWidget(self.endLineEdit)
        self.secondLayout.addWidget(self.saveLabel)
        self.secondLayout.addWidget(self.saveLineEdit)
        self.secondLayout.addWidget(self.savePathButton)

        self.thirdLayout = QHBoxLayout()
        self.thirdLayout.addWidget(self.urlIntervalLabel)
        self.thirdLayout.addWidget(self.urlIntervalLineEdit)
        self.thirdLayout.addWidget(self.urlWaitLabel)
        self.thirdLayout.addWidget(self.urlWaitLineEdit)
        self.thirdLayout.addWidget(self.calIntervalLabel)
        self.thirdLayout.addWidget(self.calIntervalLineEdit)
        self.thirdLayout.addWidget(self.calErrorWaitLabel)
        self.thirdLayout.addWidget(self.calErrorWaitLineEdit)
        self.thirdLayout.addWidget(self.tellIntervalLabel)
        self.thirdLayout.addWidget(self.tellIntervalLineEdit)
        self.thirdLayout.addWidget(self.tellErrorWaitLabel)
        self.thirdLayout.addWidget(self.tellErrorWaitLineEdit)

        self.fourLayout = QHBoxLayout()
        self.fourLayout.addWidget(self.calButton)
        self.fourLayout.addWidget(self.telButton)
        self.fourLayout.addWidget(self.allButton)
        self.fourLayout.addWidget(self.stopButton)

        self.fifLayout = QHBoxLayout()
        self.fifLayout.addWidget(self.textEdit)

        self.mainLayout = QVBoxLayout()
        self.mainLayout.addLayout(self.firstLayout)
        self.mainLayout.addLayout(self.secondLayout)
        self.mainLayout.addLayout(self.thirdLayout)
        self.mainLayout.addLayout(self.fourLayout)
        self.mainLayout.addLayout(self.fifLayout)

        self.setLayout(self.mainLayout)

        self.savePathButton.clicked.connect(self.chooseSavePath)
        self.calButton.clicked.connect(self.calStart)
        self.telButton.clicked.connect(self.telStart)
        self.allButton.clicked.connect(self.allStart)
        self.stopButton.clicked.connect(self.pause)

    def normalOutputWritten(self, text):
        self.textEdit.append(text)
        # self.textEdit.insertPlainText(self.textEdit.toPlainText()+text+'\n')
        # cursor = self.textEdit.textCursor()
        # cursor.movePosition(QtGui.QTextCursor.End)
        # cursor.insertText(text)
        # self.textEdit.setTextCursor(cursor)
        # self.textEdit.ensureCursorVisible()

    def chooseSavePath(self):
        dirname = QFileDialog.getExistingDirectory(self, "请选择文件夹", "/")
        self.saveLineEdit.setText(dirname)

    def validate(self):
        self.url = self.urlLineEdit.text()
        self.cookie = self.cookielLineEdit.text()
        self.start_date = self.startLineEdit.text()
        self.end_date = self.endLineEdit.text()
        self.urlInterval = self.urlIntervalLineEdit.text()
        self.urlWait = self.urlWaitLineEdit.text()
        self.calInterval = self.calIntervalLineEdit.text()
        self.calWait = self.calErrorWaitLineEdit.text()
        self.telInterval = self.tellIntervalLineEdit.text()
        self.telWait = self.tellErrorWaitLineEdit.text()
        self.save = self.saveLineEdit.text()
        if not self.url or not self.cookie or not self.start_date or not self.end_date or not self.save \
                or not self.urlInterval or not self.urlWait \
                or not self.calInterval or not self.calWait \
                or not self.telInterval or not self.telWait:
            QMessageBox.information(self, '参数不能为空', '参数不能为空', QMessageBox.Ok)
            return False
        if not re.match(r'\d{4}-\d{2}', self.start_date):
            QMessageBox.information(self, '开始月份格式错误(2018-01)', '开始月份格式错误(2018-01)', QMessageBox.Ok)
            return False
        if not re.match(r'\d{4}-\d{2}', self.end_date):
            QMessageBox.information(self, '结束月份格式错误(2018-01)', '结束月份格式错误(2018-01)', QMessageBox.Ok)
            return False
        if not os.path.exists(self.save) or not os.path.isdir(self.save):
            QMessageBox.information(self, '存储路径必须是一个文件夹', '存储路径必须是一个文件夹', QMessageBox.Ok)
            return False
        self.urlInterval = float(self.urlInterval)
        self.urlWait = float(self.urlWait)
        self.calInterval = float(self.calInterval)
        self.calWait = float(self.calWait)
        self.telInterval = float(self.telInterval)
        self.telWait = float(self.telWait)
        sy, sm = self.start_date.split('-')
        self.start_date = datetime.date(year=int(sy), month=int(sm), day=1)
        ey, em = self.end_date.split('-')
        self.end_date = datetime.date(year=int(ey), month=int(em), day=1)
        return True

    def start(self, cal_enable, tel_enable):
        self.textEdit.clear()
        if not self.validate():
            return
        spride.start(self.url, self.cookie, self.start_date, self.end_date, self.save,
                     self.urlInterval, self.urlWait,
                     self.calInterval, self.calWait,
                     self.telInterval, self.telWait,
                     cal_enable, tel_enable)
        self.calButton.setEnabled(False)
        self.telButton.setEnabled(False)
        self.allButton.setEnabled(False)
        self.stopButton.setEnabled(True)

    def calStart(self):
        self.start(True, False)

    def telStart(self):
        self.start(False, True)

    def allStart(self):
        self.start(True, True)

    def pause(self):
        spride.stop()
        self.calButton.setEnabled(True)
        self.telButton.setEnabled(True)
        self.allButton.setEnabled(True)
        self.stopButton.setEnabled(False)

    def closeEvent(self, event):
        spride.stop()
        QCoreApplication.quit()
        os._exit(0)

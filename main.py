import struct
import sys

import numpy as np
import matplotlib

from kafka import KafkaProducer, KafkaConsumer
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import QMainWindow, QFrame, QGridLayout, QApplication, QStyleFactory
from pip._vendor import requests

matplotlib.use("Qt5Agg")
from matplotlib.figure import Figure
from matplotlib.animation import TimedAnimation
from matplotlib.lines import Line2D
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import time
import threading


class CustomMainWindow(QMainWindow):
    def __init__(self):
        super(CustomMainWindow, self).__init__()

        self.setGeometry(300, 300, 800, 400)
        self.setWindowTitle("Data from kafka")

        self.FRAME_A = QFrame(self)
        self.FRAME_A.setStyleSheet("QWidget { background-color: %s }" % QColor(210, 210, 235, 255).name())
        self.LAYOUT_A = QGridLayout()
        self.FRAME_A.setLayout(self.LAYOUT_A)
        self.setCentralWidget(self.FRAME_A)

        self.myFig = CustomFigCanvas()
        self.LAYOUT_A.addWidget(self.myFig, *(0, 1))

        sender = threading.Thread(name='sender', target=kafka_sender, daemon=True)
        sender.start()

        fetcher = threading.Thread(name='fetcher', target=data_send_loop, daemon=True,
                                   args=(self.add_data_callback,))
        fetcher.start()

        self.show()
        return

    def add_data_callback(self, value):
        # print("Add data: " + str(value))
        self.myFig.addData(value)
        return


''' End Class '''


class CustomFigCanvas(FigureCanvas, TimedAnimation):
    def __init__(self):
        self.addedData = []
        print(matplotlib.__version__)
        # The data
        self.xlim = 200
        self.n = np.linspace(0, self.xlim - 1, self.xlim)
        a = []
        b = []
        a.append(2.0)
        a.append(4.0)
        a.append(2.0)
        b.append(4.0)
        b.append(3.0)
        b.append(4.0)
        self.y = (self.n * 0.0) + 50
        # The window
        self.fig = Figure(figsize=(5, 5), dpi=100)
        self.ax1 = self.fig.add_subplot(111)
        # self.ax1 settings
        self.ax1.set_xlabel('time')
        self.ax1.set_ylabel('raw data')
        self.line1 = Line2D([], [], color='blue')
        self.line1_tail = Line2D([], [], color='red', linewidth=2)
        self.line1_head = Line2D([], [], color='red', marker='o', markeredgecolor='r')
        self.ax1.add_line(self.line1)
        self.ax1.add_line(self.line1_tail)
        self.ax1.add_line(self.line1_head)
        self.ax1.set_xlim(0, self.xlim - 1)
        self.ax1.set_ylim(139, 141)
        FigureCanvas.__init__(self, self.fig)
        TimedAnimation.__init__(self, self.fig, interval=50, blit=True)
        return

    def new_frame_seq(self):
        return iter(range(self.n.size))

    def _init_draw(self):
        lines = [self.line1, self.line1_tail, self.line1_head]
        for l in lines:
            l.set_data([], [])
        return

    def addData(self, value):
        self.addedData.append(value)
        return

    def zoomIn(self, value):
        bottom = self.ax1.get_ylim()[0]
        top = self.ax1.get_ylim()[1]
        bottom += value
        top -= value
        self.ax1.set_ylim(bottom, top)
        self.draw()
        return

    def _step(self, *args):
        try:
            TimedAnimation._step(self, *args)
        except Exception as e:
            self.abc += 1
            print(str(self.abc))
            TimedAnimation._stop(self)
            pass
        return

    def _draw_frame(self, framedata):
        margin = 2
        while (len(self.addedData) > 0):
            self.y = np.roll(self.y, -1)
            self.y[-1] = self.addedData[0]
            del (self.addedData[0])

        self.line1.set_data(self.n[0: self.n.size - margin], self.y[0: self.n.size - margin])
        self.line1_tail.set_data(np.append(self.n[-10:-1 - margin], self.n[-1 - margin]),
                                 np.append(self.y[-10:-1 - margin], self.y[-1 - margin]))
        self.line1_head.set_data(self.n[-1 - margin], self.y[-1 - margin])
        self._drawn_artists = [self.line1, self.line1_tail, self.line1_head]
        return


''' End Class '''


class Communicate(QObject):
    data_signal = pyqtSignal(float)


''' End Class '''


def data_send_loop(add_data_callback):
    src = Communicate()
    src.data_signal.connect(add_data_callback)

    consumer = KafkaConsumer(
        'testtest',
        bootstrap_servers=['rc1a-vr3evj0gfr9plqcp.mdb.yandexcloud.net:9091','rc1b-cfed3ornaurhmno2.mdb.yandexcloud.net:9091','rc1c-pnn17rjjgu1a6ega.mdb.yandexcloud.net:9091'],
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_password="testtest",
        sasl_plain_username="test",
        ssl_cafile="yandex-ca.crt")

    for msg in consumer:
        val = struct.unpack('f', msg.value)
        src.data_signal.emit(val[0])


def kafka_sender():
    producer = KafkaProducer(
        bootstrap_servers=['rc1a-vr3evj0gfr9plqcp.mdb.yandexcloud.net:9091','rc1b-cfed3ornaurhmno2.mdb.yandexcloud.net:9091','rc1c-pnn17rjjgu1a6ega.mdb.yandexcloud.net:9091'],
        security_protocol="SASL_SSL",
        # value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_password="testtest",
        sasl_plain_username="test",
        ssl_cafile="./yandex-ca.crt")

    # Simulate some data
    n = np.linspace(0, 499, 500)
    y = 50 + 25 * (np.sin(n / 8.3)) + 10 * (np.sin(n / 7.5)) - 5 * (np.sin(n / 1.5))
    i = 0

    while True:

        time.sleep(15)
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&apikey=ZK4HK054RF13LWNR'
        r = requests.get(url)
        data = r.json()

        for key, val in data['Time Series (1min)'].items():
            v = struct.pack('f', float(val['1. open']))
            print(v)
            producer.send('testtest', v)

        if 'Time Series (1min)' in data.keys():
            val = struct.pack('f', float(list(data['Time Series (1min)'].values())[-1]['1. open']))
            # print(val)
            producer.send('testtest', val)
            i += 1


if __name__ == '__main__':
    # url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&apikey=ZK4HK054RF13LWNR'
    # r = requests.get(url)
    # data = r.json()
    #
    # print(len(data['Time Series (1min)']), list(data['Time Series (1min)'].values())[-1])

    # for key, val in data['Time Series (1min)'].items():
    #     print(key, val['1. open'])
    app = QApplication(sys.argv)
    QApplication.setStyle(QStyleFactory.create('Plastique'))
    myGUI = CustomMainWindow()
    sys.exit(app.exec_())

import threading
import time


class Spinner(threading.Thread):

    def __init__(self, msg: str, interval=0.25, cursor1='▁▂▃▄▅▆▇█', cursor2='⣾⣷⣯⣟⡿⢿⣻⣽'):
        super().__init__()
        print(f"{msg}   ", end='')
        self.status = threading.Event()
        self.interval = interval
        self.cursor1 = cursor1
        self.cursor2 = cursor2
        self.daemon = True

    def stop(self):
        self.status.set()

    def is_stopped(self):
        return self.status.is_set()

    def cursors(self, chars):
        while True:
            for cursor in chars:
                yield cursor

    def run(self):
        i = 0
        c1, c2 = self.cursors(self.cursor1), self.cursors(self.cursor2)
        p = ' '
        while not self.is_stopped():
            self.status.wait(self.interval)
            i += 1
            if i % 8 == 0:
                p = f"{p}{next(c1)}" if p == self.CURSORS_0[-1] else f"{next(c1)}"
                print(f"\b\b{p} ", end='')
            print(f"\b{next(c2)}", end='')
        print(' ')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        time.sleep(0.1)

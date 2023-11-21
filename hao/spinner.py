import threading
import time
from typing import List, Optional

from .dates import pretty_time_delta


class Spinner(threading.Thread):

    LINE_CLEAR = '\x1b[2K'
    FRAMES = [
        '|\\_________',
        '_|\\________',
        '__|\\_______',
        '___|\\______',
        '____|\\_____',
        '_____|\\____',
        '______|\\___',
        '_______|\\__',
        '________|\\_',
        '_________|\\',
        '_________/|',
        '________/|_',
        '_______/|__',
        '______/|___',
        '_____/|____',
        '____/|_____',
        '___/|______',
        '__/|_______',
        '_/|________',
        '/|_________',
    ]

    def __init__(self, msg: str, *, ps='>', done: str = '✔️', interval=0.1, frames: Optional[List[str]] = None):
        super().__init__()
        self.msg = msg
        self.ps = ps
        self.done = done
        self.interval = interval
        self.frames = frames or self.FRAMES
        self.status = threading.Event()
        self.daemon = True

    def stop(self):
        self.status.set()

    def is_stopped(self):
        return self.status.is_set()

    def cursors(self):
        while True:
            for cursor in self.frames:
                yield cursor

    def write(self, text):
        print(f"{self.LINE_CLEAR}\r{self.ps} {text}", flush=True)

    def run(self):
        start = int(time.time())
        cursors = self.cursors()
        while not self.is_stopped():
            self.status.wait(self.interval)
            took = pretty_time_delta(int(time.time()) - start, show_millis=False)
            p = f"{self.LINE_CLEAR}\r{self.ps} {self.msg} [{took}] {next(cursors)}"
            print(p, end='', flush=True)
        print(f"{self.done} ", end='\n', flush=True)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        time.sleep(0.1)

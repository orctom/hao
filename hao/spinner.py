import threading
import time
from typing import List, Optional

from .dates import pretty_time_delta


class Spinner(threading.Thread):

    LINE_CLEAR = '\x1b[2K'
    FRAMES = [
        '_________________',
        '\\________________',
        '|\\_______________',
        '_|\\______________',
        '__|\\_____________',
        '___|\\____________',
        '____|\\___________',
        '_____|\\__________',
        '______|\\_________',
        '_______|\\________',
        '________|\\_______',
        '_________|\\______',
        '__________|\\_____',
        '___________|\\____',
        '____________|\\___',
        '_____________|\\__',
        '______________|\\_',
        '_______________|\\',
        '________________|',
        '_________________',
        '_________________',
        '________________/',
        '_______________/|',
        '______________/|_',
        '_____________/|__',
        '____________/|___',
        '___________/|____',
        '__________/|_____',
        '_________/|______',
        '________/|_______',
        '_______/|________',
        '______/|_________',
        '_____/|__________',
        '____/|___________',
        '___/|____________',
        '__/|_____________',
        '_/|______________',
        '/|_______________',
        '|________________',
        '_________________',
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
        self._start = None

    def elapes(self):
        return pretty_time_delta(int(time.time()) - self._start, millis=False) if self._start else '-'

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
        self._start = int(time.time())
        cursors = self.cursors()
        while not self.is_stopped():
            self.status.wait(self.interval)
            took = self.elapes()
            p = f"{self.LINE_CLEAR}\r{self.ps} {self.msg} [{took}] {next(cursors)}"
            print(p, end='', flush=True)
        print(f"{self.done} ", end='\n', flush=True)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        time.sleep(0.1)

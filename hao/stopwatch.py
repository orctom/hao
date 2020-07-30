# -*- coding: utf-8 -*-
import time


def pretty_time_delta(seconds):
    if seconds is None:
        return '-'
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    seconds, milliseconds = divmod(seconds, 1)
    if days > 0:
        return f'{int(days)}d, {int(hours)}h, {int(minutes)}m, {int(seconds)}s'
    elif hours > 0:
        return f'{int(hours)}h, {int(minutes)}m, {int(seconds)}s'
    elif minutes > 0:
        return f'{int(minutes)}m, {int(seconds)}s'
    elif seconds > 0:
        if seconds >= 10:
            return f'{int(seconds)}s'
        else:
            return f'{int(seconds)}s, {int(milliseconds * 1000)}ms'
    else:
        return f'{int(milliseconds * 1000)}ms'


class Stopwatch(object):

    def __init__(self):
        self._start_time = None
        self._lap_time = None
        self._stop_time = None
        self.start()

    def start(self):
        self._start_time = time.time()
        self._lap_time = self._start_time
        self._stop_time = None

    def stop(self):
        if self._stop_time is None:
            self._stop_time = time.time()
            self._lap_time = self._stop_time

    def restart(self):
        _elapsed = self.elapsed()
        self._start_time = time.time()
        self._lap_time = self._start_time
        self._stop_time = None
        return _elapsed

    def reset(self):
        self._start_time = None
        self._lap_time = None
        self._stop_time = None

    def __str__(self) -> str:
        return self.elapsed()

    def __repr__(self) -> str:
        return pretty_time_delta(self._elapsed())

    def _lap(self):
        if self._lap_time is None:
            return None
        if self._stop_time is not None:
            return self._stop_time - self._lap_time
        else:
            now = time.time()
            lap_time = now - self._lap_time
            self._lap_time = now
            return lap_time

    def progress(self, percent: float, **kwargs):
        _elapsed = self._elapsed()
        if _elapsed is None:
            return 'Not started'
        _estimated = int(_elapsed / percent)
        _remain = _estimated - _elapsed
        time_elapsed = pretty_time_delta(_elapsed)
        time_remain = pretty_time_delta(_remain)
        extra_info = ', '.join([f"{k}: {v}" for k, v in kwargs.items()])
        progress = f"{percent * 100:.1f}%".rjust(6)
        time_elapsed = time_elapsed.rjust(8)
        time_remain = time_remain.rjust(8) if percent < 0.999 else '0'
        if extra_info == '':
            return f'[sw] {progress} took: {time_elapsed}, remain: {time_remain}'
        else:
            return f'[sw] {progress} took: {time_elapsed}, remain: {time_remain}, {extra_info}'

    def _elapsed(self):
        if self._start_time is None:
            return None
        if self._stop_time is not None:
            return self._stop_time - self._start_time
        else:
            return time.time() - self._start_time

    def lap(self):
        _lap = self._lap()
        if _lap is None:
            return 'Not started'
        return pretty_time_delta(_lap)

    def lap_milliseconds(self):
        _lap = self._lap()
        if _lap is None:
            return 0
        return int(_lap * 1000)

    def elapsed(self):
        _elapsed = self._elapsed()
        if _elapsed is None:
            return '0s'
        return pretty_time_delta(_elapsed)

    def elapsed_milliseconds(self):
        _elapsed = self._elapsed()
        if _elapsed is None:
            return 0
        return int(_elapsed * 1000)

    def took(self):
        return self.elapsed()

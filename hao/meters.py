# -*- coding: utf-8 -*-
import collections
import threading
import typing

import requests

from . import logs, config

LOGGER = logs.get_logger(__name__)


class SimpleCounter(object):

    def __init__(self):
        super().__init__()
        self.count: int = 0
        self.count_prev: int = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.count += 1

    def get(self):
        with self._lock:
            return self.count

    def delta(self):
        with self._lock:
            count = self.count - self.count_prev
            self.count_prev = self.count
            return count

    def reset(self):
        with self._lock:
            self.count = 0
            self.count_prev = 0

    def __str__(self, *args, **kwargs):
        return self.get()


class SimpleMetrics(object):

    def __init__(self, logger=None, interval=15):
        super().__init__()
        self._logger = logger or LOGGER
        self._interval = interval
        self._meters: typing.DefaultDict[str, SimpleCounter] = collections.defaultdict(SimpleCounter)
        self._gauges = {}
        self._reporter = PeriodicalTask(interval, self._report)
        self._n_cycle = 0
        self.prometheus_gateway = config.get('prometheus.gateway')
        self.prometheus_key = config.get('prometheus.key')

    def start(self):
        if self._reporter.is_alive():
            return self
        self.reset()
        self._reporter.start()
        return self

    def stop(self):
        if self._reporter.is_alive():
            self._reporter.stop()

    def reset(self):
        self._meters: typing.DefaultDict[str, SimpleCounter] = collections.defaultdict(SimpleCounter)
        self._n_cycle = 0

    def mark(self, key):
        self._meters[key].increment()

    def register_gauge(self, key, gauge: typing.Callable, overwrite=True):
        if not overwrite and key in self._gauges:
            return
        self._gauges[key] = gauge

    def _report(self):
        try:
            self._report_meters()
            self._report_gauges()
        except Exception as e:
            self._logger.error(e)

    def _report_meters(self):
        self._n_cycle += 1
        for key, counter in self._meters.copy().items():
            delta = counter.delta()
            rate = delta / self._interval
            total = counter.get()
            rate_total = total / (self._interval * self._n_cycle)
            self._logger.info(f"[meter-{key}] count: {delta}, rate: {rate:.2f} it/s; total: {total}, rate: {rate_total:.2f} it/s")
            self._report_to_prometheus(key, rate)

    def _report_gauges(self):
        for key, gauge in self._gauges.copy():
            try:
                value = gauge()
                self._logger.info(f"[{key}] gauge: {value}")
            except Exception as e:
                self._logger.warning(e)

    def _report_to_prometheus(self, job_name, value):
        if self.prometheus_gateway and self.prometheus_key:
            url = f"{self.prometheus_gateway}/metrics/job/{job_name}/instance/{config.HOSTNAME}"
            data = f'''# TYPE {self.prometheus_key} gauge\n{self.prometheus_key} {value}\n'''.encode()
            try:
                requests.put(url, data=data, timeout=5)
            except Exception as e:
                LOGGER.info(e)


class PeriodicalTask(threading.Thread):

    def __init__(self, interval, target):
        super().__init__()
        self.status = threading.Event()
        self.interval = interval
        self.target = target
        self.daemon = True

    def stop(self):
        self.status.set()

    def is_stopped(self):
        return self.status.isSet()

    def run(self):
        while not self.is_stopped():
            LOGGER.debug('event')
            self.status.wait(self.interval)
            self.target()

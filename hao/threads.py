from threading import Event, Thread
from typing import Callable, Optional

from . import logs

LOGGER = logs.get_logger(__name__)


class PeriodicalTask(Thread):

    def __init__(self, interval: int, function: Callable, on_error: Optional[Callable] = None):
        super().__init__()
        self.status = Event()
        self.interval = interval
        self.function = function
        self.on_error = on_error
        self.daemon = True

    def stop(self):
        self.status.set()

    def is_stopped(self):
        return self.status.isSet()

    def run(self):
        while not self.is_stopped():
            LOGGER.debug('event')
            self.status.wait(self.interval)
            try:
                self.function()
            except Exception as ex:
                if self.on_error:
                    self.on_error(ex)
                else:
                    LOGGER.exception(ex)

import asyncio
from typing import Callable

from . import logs

LOGGER = logs.get_logger(__name__)


class PeriodicalTask:

    def __init__(self, interval: int, function: Callable, on_error: Callable | None = None):
        self._interval = interval
        self._function = function
        self._on_error = on_error
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    def start(self):
        if self._task is not None and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    def stop(self):
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()

    def is_alive(self):
        return self._task is not None and not self._task.done()

    async def _run(self):
        while not self._stopped.is_set():
            try:
                if asyncio.iscoroutinefunction(self._function):
                    await self._function()
                else:
                    self._function()
            except asyncio.CancelledError:
                break
            except Exception as ex:
                if self._on_error:
                    if asyncio.iscoroutinefunction(self._on_error):
                        await self._on_error(ex)
                    else:
                        self._on_error(ex)
                else:
                    import logging
                    LOGGER = logging.getLogger(__name__)
                    LOGGER.exception(ex)
            await asyncio.sleep(self._interval)

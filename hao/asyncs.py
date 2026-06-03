# -*- coding: utf-8 -*-
import asyncio
import contextvars
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Coroutine

_GLOBAL_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, os.cpu_count()),
    thread_name_prefix='AsyncExecutor'
)

def is_in_main_thread():
    return threading.current_thread().__class__.__name__ == '_MainThread'


def get_event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return asyncio.get_event_loop()


def run(coroutine: Coroutine, fire_and_forget: bool = False):
    if not asyncio.iscoroutine(coroutine):
        raise TypeError(f"coroutine expected, but got {type(coroutine).__name__}")

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)
    else:
        current_context = contextvars.copy_context()

        def _run_in_new_loop():
            return current_context.run(asyncio.run, coroutine)

        future = _GLOBAL_EXECUTOR.submit(_run_in_new_loop)
        if fire_and_forget:
            return
        return future.result()

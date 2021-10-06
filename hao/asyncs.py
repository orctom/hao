# -*- coding: utf-8 -*-
import asyncio
import threading


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

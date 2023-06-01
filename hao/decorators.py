# -*- coding: utf-8 -*-

import asyncio
import functools
import logging
import signal
import threading
import time
import typing

from decorator import decorator

from . import asyncs, exceptions
from .stopwatch import Stopwatch

LOGGER = logging.getLogger(__name__)


@decorator
def retry(func, exceptions=Exception, tries=2, delay=0.5, backoff=1.2, logger=LOGGER, *a, **kw):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        func: wrapper function
        exceptions: The exception to check. may be a tuple of exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay each retry).
        logger: Logger to use. If None, print.
    """

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            n_tried, delays = 0, delay
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    n_tried += 1
                    if n_tried > tries:
                        raise e
                    logger.warning(f"{e}, Retrying {n_tried} of {tries} in {delays} seconds...")
                    asyncio.sleep(delays)
                    delays *= backoff
    else:
        def wrapper(*args, **kwargs):
            n_tried, delays = 0, delay
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    n_tried += 1
                    if n_tried > tries:
                        raise e
                    logger.warning(f"{e}, Retrying {n_tried} of {tries} in {delays} seconds...")
                    time.sleep(delays)
                    delays *= backoff
    return wrapper(*a, **kw)


@decorator
def try_except(func, callback=None, throw=False, logger=LOGGER, *a, **kw):

    def exception_handler(exception: Exception, *args, **kwargs):
        logger.exception(exception)
        if callback is not None:
            callback(*args, **kwargs)
        if throw:
            raise exception

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                exception_handler(e, *args, **kwargs)
    else:
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                exception_handler(e, *args, **kwargs)
    return wrapper(*a, **kw)


@decorator
def timer(func, logger=LOGGER, *a, **kw):

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            sw = Stopwatch()
            res = await func(*args, **kwargs)
            logger.info(f'[{func.__name__}] took {sw.took()}')
            return res
    else:
        def wrapper(*args, **kwargs):
            sw = Stopwatch()
            res = func(*args, **kwargs)
            logger.info(f'[{func.__name__}] took {sw.took()}')
            return res
    return wrapper(*a, **kw)


@decorator
def synchronized(func, *a, **kw):
    func.__lock__ = threading.Lock()

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            with func.__lock__:
                return await func(*args, **kwargs)
    else:
        def wrapper(*args, **kwargs):
            with func.__lock__:
                return func(*args, **kwargs)
    return wrapper(*a, **kw)


@decorator
def background(func, *a, **kw):
    try:
        import contextvars  # Python 3.7+ only.
    except ImportError:
        contextvars = None

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
    else:
        def wrapper(*args, **kwargs):
            if asyncs.is_in_main_thread():
                loop = asyncs.get_event_loop()
                if contextvars is not None:
                    # Ensure we run in the same context
                    context = contextvars.copy_context()
                    f = functools.partial(context.run, func, *args, **kwargs)
                    args = []
                elif kwargs:
                    # loop.run_in_executor doesn't accept 'kwargs', so bind them in here
                    f = functools.partial(func, **kwargs)
                else:
                    f = func
                return loop.run_in_executor(None, f, *args)
            else:
                return func(*args, **kwargs)
    return wrapper(*a, **kw)


@decorator
def timeout(func: typing.Callable, seconds=5, timeout_exception=TimeoutError, message=None, *a, **kw):
    """
    NOT work for windows, signal not supported??!!
    """
    def handle(_, __):
        msg = message or f'{func.__name__}() timed out ({seconds} seconds)'
        exceptions.throw(timeout_exception, msg)

    if asyncio.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handle)
            signal.alarm(seconds)
            try:
                return await func(*args, **kwargs)
            finally:
                signal.signal(signal.SIGALRM, old)
                signal.alarm(0)
    else:
        def wrapper(*args, **kwargs):
            if asyncs.is_in_main_thread():
                old = signal.signal(signal.SIGALRM, handle)
                signal.alarm(seconds)
                try:
                    return func(*args, **kwargs)
                finally:
                    signal.signal(signal.SIGALRM, old)
                    signal.alarm(0)
            else:
                return func(*args, **kwargs)
    return wrapper(*a, **kw)

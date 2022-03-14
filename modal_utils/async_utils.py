import asyncio
import concurrent.futures
import functools
import inspect
import sys
import time
from typing import Any, Coroutine, List

import synchronicity

from .logger import logger

synchronizer = synchronicity.Synchronizer()
# atexit.register(synchronizer.close)


def asyncio_run(coro):
    # 3.6 compatibility version of asyncio.run
    if sys.version_info >= (3, 7):
        return asyncio.run(coro)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def retry(direct_fn=None, *, n_attempts=3, base_delay=0, delay_factor=2, timeout=90, warn_on_cancel=True):
    """Decorator that calls an async function multiple times, with a given timeout.

    If a `base_delay` is provided, the function is given an exponentially
    increasing delay on each run, up until the maximum number of attempts.

    Usage:

    ```
    @retry
    async def may_fail_default():
        # ...
        pass

    @retry(n_attempts=5, base_delay=1)
    async def may_fail_delay():
        # ...
        pass
    ```
    """

    def decorator(fn):
        @functools.wraps(fn)
        async def f_wrapped(*args, **kwargs):
            delay = base_delay
            for i in range(n_attempts):
                t0 = time.time()
                try:
                    return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout)
                except asyncio.CancelledError:
                    if warn_on_cancel:
                        logger.warning(f"Function {fn} was cancelled")
                    raise
                except Exception as e:
                    if i >= n_attempts - 1:
                        raise
                    logger.warning(
                        f"Failed invoking function {fn}: {repr(e)}"
                        f" (took {time.time() - t0}s, sleeping {delay}s"
                        f" and trying {n_attempts - i - 1} more times)"
                    )
                await asyncio.sleep(delay)
                delay *= delay_factor

        return f_wrapped

    if direct_fn is not None:
        # It's invoked like @retry
        return decorator(direct_fn)
    else:
        # It's invoked like @retry(n_attempts=...)
        return decorator


def add_traceback(obj, func_name=None):
    """Wrap a function/generator and make sure its traceback is always printed."""
    if func_name is None:
        func_name = repr(obj)
    if inspect.iscoroutine(obj):

        async def _wrap_coro():
            try:
                return await obj
            # pre Python3.8, CancelledErrors were a subclass of exception
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"Exception while running {func_name}")
                raise

        return _wrap_coro()
    elif inspect.isasyncgen(obj):

        async def _wrap_gen():
            try:
                async for elm in obj:
                    yield elm
            # pre Python3.8, CancelledErrors were a subclass of exception
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"Exception while running {func_name}")
                raise

        return _wrap_gen()
    else:
        raise Exception(f"{obj} is not a coro or async gen!")


async def chunk_generator(generator, timeout):
    """Takes a generator and returns a generator of generator where each sub-generator only runs for a certain time.

    TODO: merge this into aiostream.
    """
    done = False
    task = None
    try:
        while not done:

            async def chunk():
                nonlocal done, task
                t0 = time.time()
                while True:
                    try:
                        attempt_timeout = t0 + timeout - time.time()
                        if task is None:
                            coro = generator.__anext__()
                            loop = asyncio.get_event_loop()
                            task = loop.create_task(coro)
                        value = await asyncio.wait_for(asyncio.shield(task), attempt_timeout)
                        yield value
                        task = None
                    except asyncio.TimeoutError:
                        return
                    except StopAsyncIteration:
                        done = True
                        return

            yield chunk()
    finally:
        if task is not None:
            task.cancel()


# TODO: maybe these methods could move into synchronizer later?


def asyncify_generator(generator_fn):
    """Takes a blocking generator and returns an async generator.

    TODO: merge into aiostream: https://github.com/vxgmichel/aiostream/issues/78
    """

    @functools.wraps(generator_fn)
    async def new_generator(*args, **kwargs):
        generator = generator_fn(*args, **kwargs)
        loop = asyncio.get_event_loop()
        done = False

        def safe_next(it):
            nonlocal done
            try:
                return next(it)
            except StopIteration:
                done = True

        while True:
            ret = await loop.run_in_executor(None, safe_next, generator)
            if done:
                break
            yield ret

    return new_generator


def asyncify_function(function):
    async def asynced_function(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: function(*args, **kwargs))

    return asynced_function


class TaskContext:
    """Simple thing to make sure we don't have stray tasks.

    Usage:
    async with TaskContext() as task_context:
        task = task_context.create(coro())
    """

    def __init__(self, grace=None):
        self._grace = grace
        self._loops = set()

    async def start(self):
        # TODO: this only exists as a standalone method because Client doesn't have a proper ctx mgr
        self._tasks = set()
        self._exited = asyncio.Event()  # Used to stop infinite loops

    async def __aenter__(self):
        await self.start()
        return self

    async def stop(self):
        self._exited.set()
        await asyncio.sleep(0)  # Causes any just-created tasks to get started
        unfinished_tasks = [t for t in self._tasks if not t.done()]
        gather_future = None
        try:
            if self._grace is not None and unfinished_tasks:
                gather_future = asyncio.gather(*unfinished_tasks, return_exceptions=True)
                await asyncio.wait_for(gather_future, timeout=self._grace)
        except asyncio.TimeoutError:
            pass
        finally:
            await asyncio.sleep(0)  # Needed in 3.6 to make any just-cancelled tasks actually cancel
            # asyncio.wait_for cancels the future, but the CancelledError
            # still needs to be handled
            # (https://stackoverflow.com/a/63356323/2475114)
            if gather_future:
                try:
                    await gather_future
                # pre Python3.8, CancelledErrors were a subclass of exception
                except asyncio.CancelledError:
                    pass

            for task in self._tasks:
                if task.done() and not task.cancelled():
                    # Raise any exceptions if they happened.
                    # Only tasks without a done_callback will still be present in self._tasks
                    task.result()

                if task.done() or task in self._loops:
                    continue

                logger.warning(f"Canceling unfinished task {task}")
                task.cancel()

    async def __aexit__(self, exc_type, value, tb):
        await self.stop()

    def create_task(self, coro_or_task):
        if isinstance(coro_or_task, asyncio.Task):
            task = coro_or_task
        elif asyncio.iscoroutine(coro_or_task):
            loop = asyncio.get_event_loop()
            task = loop.create_task(coro_or_task)
        else:
            raise Exception(f"Object of type {type(coro_or_task)} is not a coroutine or Task")
        self._tasks.add(task)
        return task

    def infinite_loop(self, async_f, timeout=90, sleep=10) -> Coroutine[Any, Any, None]:
        function_name = async_f.__qualname__

        async def loop_coro() -> None:
            logger.debug(f"Starting infinite loop {function_name}")
            while True:
                try:
                    await asyncio.wait_for(async_f(), timeout=timeout)
                # pre Python3.8, CancelledErrors were a subclass of exception
                except asyncio.CancelledError:
                    break
                except Exception:
                    logger.exception(f"Loop attempt failed for {function_name}")
                try:
                    await asyncio.wait_for(self._exited.wait(), timeout=sleep)
                except asyncio.TimeoutError:
                    continue
                logger.debug(f"Exiting infinite loop for {function_name}")
                break

        t = self.create_task(loop_coro())
        if hasattr(t, "set_name"):  # Was added in Python 3.8:
            t.set_name(f"{function_name} loop")
        self._loops.add(t)
        return t

    async def wait(self, *tasks):
        # Waits until all of tasks have finished
        # This is slightly different than asyncio.wait since the `tasks` argument
        # may be a subset of all the tasks.
        # If any of the task context's task raises, throw that exception
        # This is probably O(n^2) sadly but I guess it's fine
        unfinished_tasks = set(tasks)
        while True:
            unfinished_tasks &= self._tasks
            if not unfinished_tasks:
                break
            try:
                done, pending = await asyncio.wait_for(
                    asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED), timeout=30.0
                )
            except asyncio.TimeoutError:
                continue
            for task in done:
                task.result()  # Raise exception if needed
                if task in unfinished_tasks:
                    unfinished_tasks.remove(task)
                if task in self._tasks:
                    self._tasks.remove(task)


def run_coro_blocking(coro):
    """Fairly hacky thing that's needed in some extreme cases.

    It's basically works like asyncio.run but unlike asyncio.run it also works
    with in the case an event loop is already running. It does this by basically
    moving the whole thing to a separate thread.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        fut = executor.submit(asyncio_run, coro)
        return fut.result()


async def queue_batch_iterator(q: asyncio.Queue, max_batch_size=100, debounce_time=0.015):
    """
    Read from a queue but return lists of items when queue is large
    """
    item_list: List[Any] = []

    while True:
        if q.empty() and len(item_list) > 0:
            yield item_list
            item_list = []
            await asyncio.sleep(debounce_time)

        res = await q.get()

        if len(item_list) >= max_batch_size:
            yield item_list
            item_list = []

        if res is None:
            yield item_list
            break
        item_list.append(res)
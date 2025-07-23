import asyncio
import logging
import signal
import time
from collections.abc import Awaitable
from typing import Callable, Literal, Union, cast, overload

from script_utils_log_async.logging_setup import setup_logging, trigger_shutdown_filter

AsyncMainCoroutine = Callable[[asyncio.Event], Awaitable[None]]
SyncMainCoroutine = Callable[[], None]
MainCoroutine = Callable[..., Union[Awaitable[None], None]]


@overload
def setup_main(
    *,
    is_async: Literal[True],
    setup_logging_func: Callable[[], None] = ...,
    do_on_shutdown_catch: Callable[[], None] = ...,
) -> Callable[[AsyncMainCoroutine], Callable[[], Awaitable[None]]]: ...
@overload
def setup_main(
    *,
    is_async: Literal[False],
    setup_logging_func: Callable[[], None] = ...,
    do_on_shutdown_catch: Callable[[], None] = ...,
) -> Callable[[SyncMainCoroutine], Callable[[], None]]: ...


def setup_main(
    *,
    is_async: bool,
    setup_logging_func: Callable[[], None] = setup_logging,
    do_on_shutdown_catch: Callable[[], None] = trigger_shutdown_filter,
) -> Callable[..., Callable[[], Union[Awaitable[None], None]]]:
    def decorator(main_coroutine: MainCoroutine):
        async def wrapper():
            setup_logging_func()

            print("=" * 70)
            logging.info("Script started.")
            print("=" * 70)
            start_time = time.time()

            try:
                if is_async:
                    loop = asyncio.get_running_loop()
                    stop_event = asyncio.Event()

                    def on_shutdown():
                        logging.warning(
                            "Shutdown signal received! Saving prograss and shutting down."
                        )
                        do_on_shutdown_catch()
                        stop_event.set()

                    signals_to_handle: list[int] = [
                        signal.SIGINT,
                        signal.SIGTERM,
                        signal.SIGHUP,
                    ]
                    for sig in signals_to_handle:
                        loop.add_signal_handler(sig, on_shutdown)

                    coroutine = cast(AsyncMainCoroutine, main_coroutine)
                    await coroutine(stop_event)
                else:
                    coroutine = cast(SyncMainCoroutine, main_coroutine)
                    coroutine()
            except Exception:
                logging.exception("Unhandled exception occurred in main.")
            finally:
                end_time = round(time.time() - start_time, 2)
                logging.info(f"Program exiting after {end_time} seconds.")
                print()

        return wrapper

    return decorator

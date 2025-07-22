import asyncio
import logging
import signal
import time
from collections.abc import Awaitable
from typing import Callable, Literal, Union, cast, overload

from script_utils_log_async.logging_setup import setup_logging, trigger_shutdown_filter

AsyncMainCoroutine = Callable[[asyncio.Event], Awaitable[None]]
MainCoroutine = Callable[[], None]


@overload
def setup_main(
    main_coroutine: AsyncMainCoroutine,
    is_async: Literal[True],
    *,
    setup_logging_func: Callable[[], None] = ...,
) -> Callable[[], Awaitable[None]]: ...
@overload
def setup_main(
    main_coroutine: MainCoroutine,
    is_async: Literal[False],
    *,
    setup_logging_func: Callable[[], None] = ...,
) -> Callable[[], Awaitable[None]]: ...


def setup_main(
    main_coroutine: Callable[..., Union[Awaitable[None], None]],
    is_async: bool,
    *,
    setup_logging_func: Callable[[], None] = setup_logging,
) -> Callable[[], Awaitable[None]]:
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
                    trigger_shutdown_filter()
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
                coroutine = cast(MainCoroutine, main_coroutine)
                coroutine()
        except Exception:
            logging.exception("Unhandled exception occurred in main.")
        finally:
            end_time = round(time.time() - start_time, 2)
            logging.info(f"Program exiting after {end_time} seconds.")
            print()

    return wrapper

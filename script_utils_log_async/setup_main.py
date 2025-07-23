import asyncio
import logging
import signal
import time
from collections.abc import Coroutine
from typing import Callable, Literal, Union, cast, overload

from script_utils_log_async.logging_setup import setup_logging, trigger_shutdown_filter

VoidFun = Callable[[], None]
AsyncMainCoroutine = Callable[[], Coroutine[object, object, None]]
SyncMainCoroutine = VoidFun
MainCoroutine = Callable[..., Union[Coroutine[object, object, None], None]]


def my_start_end_decorator(log_time: bool, is_async: bool):
    def decorator(func: MainCoroutine) -> Union[AsyncMainCoroutine, SyncMainCoroutine]:
        async def async_wrapper():
            print("=" * 70)
            logging.info("Script started.")
            print("=" * 70)
            start_time = None
            if log_time:
                start_time = time.time()

            try:
                if is_async:
                    main = cast(AsyncMainCoroutine, func)
                    await main()
                else:
                    main = cast(SyncMainCoroutine, func)
                    main()
            except Exception:
                logging.exception("Unhandled exception occurred in main.")
            finally:
                if log_time and start_time:
                    end_time = round(time.time() - start_time, 2)
                    logging.info(f"Program exiting after {end_time} seconds.")
                print()

        def sync_wrapper():
            asyncio.run(async_wrapper())

        if is_async:
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def my_on_shutdown_catch():
    logging.warning("Shutdown signal received! Saving prograss and shutting down.")
    trigger_shutdown_filter()


@overload
def setup_main(
    *,
    is_async: Literal[True],
    log_time: bool = ...,
    start_end_decorator: Callable[
        [bool, bool], Callable[[MainCoroutine], SyncMainCoroutine]
    ] = ...,
    setup_logging_func: VoidFun = ...,
    on_shutdown_catch: VoidFun = ...,
    on_cancel: VoidFun = ...,
    on_finish: VoidFun = ...,
) -> Callable[[AsyncMainCoroutine], VoidFun]: ...
@overload
def setup_main(
    *,
    is_async: Literal[False],
    log_time: bool = ...,
    start_end_decorator: Callable[
        [bool, bool], Callable[[MainCoroutine], AsyncMainCoroutine]
    ] = ...,
    setup_logging_func: VoidFun = ...,
    on_shutdown_catch: VoidFun = ...,
) -> Callable[[SyncMainCoroutine], VoidFun]: ...
def setup_main(
    *,
    is_async: bool,
    log_time: bool = True,
    start_end_decorator: Callable[
        [bool, bool],
        Callable[[MainCoroutine], Union[AsyncMainCoroutine, SyncMainCoroutine]],
    ] = my_start_end_decorator,
    setup_logging_func: VoidFun = setup_logging,
    on_shutdown_catch: VoidFun = my_on_shutdown_catch,
    on_cancel: VoidFun = lambda: logging.info("Currently running task was cancelled."),
    on_finish: VoidFun = lambda: logging.info("Current task completed normally."),
) -> Callable[..., Callable[[], Union[Coroutine[object, object, None], None]]]:
    def decorator(main_coroutine: MainCoroutine):
        async def async_wrapper():
            setup_logging_func()

            if is_async:
                async_start_end_decorator = cast(
                    Callable[
                        [bool, bool], Callable[[MainCoroutine], AsyncMainCoroutine]
                    ],
                    start_end_decorator,
                )

                @async_start_end_decorator(log_time, is_async)
                async def async_main():
                    loop = asyncio.get_running_loop()
                    stop_event = asyncio.Event()

                    def on_shutdown():
                        on_shutdown_catch()
                        stop_event.set()

                    signals_to_handle: list[int] = [
                        signal.SIGINT,
                        signal.SIGTERM,
                        signal.SIGHUP,
                    ]
                    for sig in signals_to_handle:
                        loop.add_signal_handler(sig, on_shutdown)

                    async_main_coroutine = cast(AsyncMainCoroutine, main_coroutine)
                    main_task = asyncio.create_task(async_main_coroutine())

                    _ = await asyncio.wait(
                        {main_task, asyncio.create_task(stop_event.wait())},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_event.is_set():
                        _ = main_task.cancel()
                        try:
                            await main_task
                        except asyncio.CancelledError:
                            on_cancel()
                    else:
                        on_finish()

                await async_main()
            else:
                decorated_main_coroutine = start_end_decorator(log_time, is_async)(
                    main_coroutine
                )
                sync_main_coroutine = cast(SyncMainCoroutine, decorated_main_coroutine)
                sync_main_coroutine()

        def sync_wrapper():
            asyncio.run(async_wrapper())

        return sync_wrapper

    return decorator

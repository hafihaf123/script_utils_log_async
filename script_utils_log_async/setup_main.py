import asyncio
import logging
import signal
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Callable, Union, cast

from script_utils_log_async.logging_setup import setup_logging, trigger_shutdown_filter

VoidFun = Callable[[], None]
AsyncMainCoroutine = Callable[[], Coroutine[object, object, None]]
SyncMainCoroutine = VoidFun
MainCoroutine = Callable[..., Union[Coroutine[object, object, None], None]]


@dataclass
class SetupMainConfig:
    @staticmethod
    def my_on_main_start() -> None:
        print("=" * 70)
        logging.info("Script started.")
        print("=" * 70)

    @staticmethod
    def my_on_shutdown_catch() -> None:
        logging.warning("Shutdown signal received. Shutting down.")
        trigger_shutdown_filter()

    is_async: bool
    log_time: bool = True
    on_main_start: VoidFun = my_on_main_start
    on_main_exception: VoidFun = lambda: logging.exception(
        "Unhandled exception occurred in main."
    )
    on_shutdown_catch: VoidFun = my_on_shutdown_catch
    on_cancel: VoidFun = lambda: logging.info("Currently running task was cancelled.")
    on_finish: VoidFun = lambda: logging.info("Current task completed normally.")
    setup_logging: VoidFun = setup_logging


def start_end_decorator(
    config: SetupMainConfig,
) -> Callable[[MainCoroutine], SyncMainCoroutine]:
    def decorator(func: MainCoroutine) -> SyncMainCoroutine:
        async def async_wrapper() -> None:
            config.on_main_start()
            start_time = None
            if config.log_time:
                start_time = time.time()

            try:
                if config.is_async:
                    main = cast(AsyncMainCoroutine, func)
                    await main()
                else:
                    main = cast(SyncMainCoroutine, func)
                    main()
            except Exception:
                config.on_main_exception()
            finally:
                if config.log_time and start_time:
                    end_time = round(time.time() - start_time, 2)
                    logging.info(f"Script ended after {end_time} seconds.")
                print()

        def sync_wrapper() -> None:
            asyncio.run(async_wrapper())

        return sync_wrapper

    return decorator


def setup_main(
    config: SetupMainConfig,
) -> Callable[[MainCoroutine], SyncMainCoroutine]:
    def decorator(main_coroutine: MainCoroutine) -> VoidFun:
        def async_wrapper() -> None:
            config.setup_logging()

            @start_end_decorator(config)
            async def async_main() -> None:
                loop = asyncio.get_running_loop()
                stop_event = asyncio.Event()

                def on_shutdown():
                    config.on_shutdown_catch()
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
                        config.on_cancel()
                else:
                    config.on_finish()

            async_main()

        def sync_wrapper() -> None:
            config.setup_logging()
            decorated_main_coroutine = start_end_decorator(config)(main_coroutine)
            decorated_main_coroutine()

        if config.is_async:
            return async_wrapper
        return sync_wrapper

    return decorator

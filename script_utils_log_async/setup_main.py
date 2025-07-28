import asyncio
import logging
import signal
import time
from collections.abc import Coroutine
from enum import Enum
from typing import Callable, Literal, Union, cast, overload

from script_utils_log_async.logging_setup import setup_logging, trigger_shutdown_filter

VoidFun = Callable[[], None]
AsyncMainCoroutine = Callable[[], Coroutine[object, object, None]]
SyncMainCoroutine = VoidFun
MainCoroutine = Callable[..., Union[Coroutine[object, object, None], None]]


class _Unset(Enum):
    token = 0


_UNSET = _Unset.token


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

    @overload
    def __init__(
        self,
        *,
        is_async: Literal[True],
        log_time: bool = ...,
        on_main_start: VoidFun = ...,
        on_main_end: VoidFun = ...,
        on_main_exception: VoidFun = ...,
        on_shutdown_catch: VoidFun = ...,
        on_cancel: VoidFun = ...,
        on_finish: VoidFun = ...,
        setup_logging_fun: VoidFun = lambda: setup_logging(shutdown_filter=True),
        use_uvloop: bool = ...,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        is_async: Literal[False],
        log_time: bool = ...,
        on_main_start: VoidFun = ...,
        on_main_end: VoidFun = ...,
        on_main_exception: VoidFun = ...,
        setup_logging_fun: VoidFun = setup_logging,
    ) -> None: ...

    def __init__(
        self,
        *,
        is_async: bool,
        log_time: bool = True,
        on_main_start: VoidFun = my_on_main_start,
        on_main_end: VoidFun = lambda: None,
        on_main_exception: VoidFun = lambda: logging.exception(
            "Unhandled exception occurred in main."
        ),
        on_shutdown_catch: VoidFun = my_on_shutdown_catch,
        on_cancel: VoidFun = lambda: logging.info(
            "Currently running task was cancelled."
        ),
        on_finish: VoidFun = lambda: logging.info("Current task completed normally."),
        setup_logging_fun: Union[VoidFun, _Unset] = _UNSET,
        use_uvloop: bool = True,
    ):
        self.is_async: bool = is_async
        self.log_time: bool = log_time
        self.on_main_start: VoidFun = on_main_start
        self.on_main_end: VoidFun = on_main_end
        self.on_main_exception: VoidFun = on_main_exception
        self.on_shutdown_catch: VoidFun = on_shutdown_catch
        self.on_cancel: VoidFun = on_cancel
        self.on_finish: VoidFun = on_finish
        if setup_logging_fun is _UNSET:
            setup_logging_fun = (
                (lambda: setup_logging(shutdown_filter=True))
                if is_async
                else setup_logging
            )
        self.setup_logging_fun: VoidFun = setup_logging_fun
        self.use_uvloop: bool = use_uvloop
        self.post_main_callbacks: list[VoidFun] = []

    def register_post_main_callback(self, callback: VoidFun) -> None:
        self.post_main_callbacks.append(callback)


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
                for callback in config.post_main_callbacks:
                    try:
                        callback()
                    except Exception:
                        logging.exception("Post-main callback failed.")
                config.on_main_end()
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
            config.setup_logging_fun()

            if config.use_uvloop:
                try:
                    import uvloop

                    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                    logging.debug("Set 'uvloop' as the default loop policy.")
                except ImportError:
                    logging.warning(
                        "Uvloop not available, falling back to default asyncio loop."
                    )

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
            config.setup_logging_fun()
            decorated_main_coroutine = start_end_decorator(config)(main_coroutine)
            decorated_main_coroutine()

        if config.is_async:
            return async_wrapper
        return sync_wrapper

    return decorator

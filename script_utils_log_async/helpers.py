import asyncio
import logging
from typing import TYPE_CHECKING, Optional, cast

if TYPE_CHECKING:
    import httpx


async def fetch_with_retry(
    client: "httpx.AsyncClient",
    url: str,
    params: Optional[dict[str, str]] = None,
    retries: int = 5,
    initial_backoff: int = 5,
) -> "httpx.Response":
    import httpx

    async def backoff(attempt: int):
        nonlocal initial_backoff
        backoff_time = initial_backoff * attempt
        logging.warning(
            f"Request to '{url}' failed. Using backoff: {backoff_time} seconds."
        )
        await asyncio.sleep(backoff_time)

    for attempt in range(retries):
        try:
            response = await client.get(url, params=params)
            _ = response.raise_for_status()
            return response
        except httpx.RequestError:
            if attempt == retries - 1:
                raise Exception(f"Request to '{url}' failed after {retries} attempts")
            await backoff(attempt + 1)
            continue
        except httpx.HTTPStatusError as status_exception:
            if attempt == retries - 1:
                raise Exception(f"Request to '{url}' failed after {retries} attempts")
            retry_after = cast(
                str, status_exception.response.headers.get("Retry-After")
            )
            if not retry_after:
                await backoff(attempt + 1)
                continue

            retry_after_seconds = int(retry_after)
            if retry_after_seconds > 60:
                raise Exception("Too long retry time")
            logging.warning(
                f"Rate limited. Retrying after {retry_after_seconds} seconds."
            )
            await asyncio.sleep(retry_after_seconds)
    raise

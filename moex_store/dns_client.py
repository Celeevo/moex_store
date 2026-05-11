import asyncio
import socket

import aiohttp
from aiomoex.client import ISSClient, ISSMoexError


class DNS_ISSClient(ISSClient):
    retry_attempts = 3
    retry_delay = 1

    async def get(self, start=None):
        """Load one MOEX ISS page with retries for transient network errors.

        DNS, SSL, IPv4 and timeouts are configured by the aiohttp session that
        MoexStore passes into aiomoex. This wrapper must not replace the
        session connector or return None, because aiomoex expects a dict.
        """
        last_error = None
        retryable_errors = (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            socket.gaierror,
            OSError,
        )

        for attempt in range(1, self.retry_attempts + 1):
            try:
                result = await super().get(start)
                # aiomoex дальше ожидает словарь и падает на respond.get(...),
                # поэтому пустой ответ MOEX переводим в явную retryable ошибку.
                if result is None:
                    raise aiohttp.ClientPayloadError(
                        f"MOEX ISS returned empty response: url={self._url}, start={start}"
                    )
                return result
            except ISSMoexError:
                raise
            except retryable_errors as error:
                last_error = error
                if attempt == self.retry_attempts:
                    break
                await asyncio.sleep(self.retry_delay * attempt)

        raise ISSMoexError(
            f"MOEX request failed after {self.retry_attempts} attempts: "
            f"url={self._url}, start={start}, error={last_error}"
        ) from last_error

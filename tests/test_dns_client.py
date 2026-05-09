import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

from aiomoex.client import ISSMoexError
from moex_store.dns_client import DNS_ISSClient


class DNSISSClientTests(unittest.TestCase):
    def setUp(self):
        self.previous_retry_attempts = DNS_ISSClient.retry_attempts
        self.previous_retry_delay = DNS_ISSClient.retry_delay
        DNS_ISSClient.retry_attempts = 3
        DNS_ISSClient.retry_delay = 1

    def tearDown(self):
        DNS_ISSClient.retry_attempts = self.previous_retry_attempts
        DNS_ISSClient.retry_delay = self.previous_retry_delay

    def test_retries_transient_network_errors(self):
        client = DNS_ISSClient(object(), "https://iss.moex.com/test.json", {})

        with patch(
            "moex_store.dns_client.ISSClient.get",
            new=AsyncMock(
                side_effect=[
                    aiohttp.ClientConnectionError("temporary"),
                    {"candles": []},
                ]
            ),
        ) as base_get, patch(
            "moex_store.dns_client.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            result = asyncio.run(client.get(start=100))

        self.assertEqual(result, {"candles": []})
        self.assertEqual(base_get.await_count, 2)
        sleep.assert_awaited_once_with(1)

    def test_retries_empty_moex_response(self):
        client = DNS_ISSClient(object(), "https://iss.moex.com/test.json", {})

        with patch(
            "moex_store.dns_client.ISSClient.get",
            new=AsyncMock(side_effect=[None, {"candles": []}]),
        ) as base_get, patch(
            "moex_store.dns_client.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            result = asyncio.run(client.get(start=47500))

        self.assertEqual(result, {"candles": []})
        self.assertEqual(base_get.await_count, 2)
        sleep.assert_awaited_once_with(1)

    def test_raises_moex_error_instead_of_returning_none_after_retries(self):
        client = DNS_ISSClient(object(), "https://iss.moex.com/test.json", {})

        with patch(
            "moex_store.dns_client.ISSClient.get",
            new=AsyncMock(side_effect=aiohttp.ClientConnectionError("temporary")),
        ), patch(
            "moex_store.dns_client.asyncio.sleep",
            new=AsyncMock(),
        ):
            with self.assertRaisesRegex(ISSMoexError, "MOEX request failed after 3 attempts"):
                asyncio.run(client.get(start=47500))


if __name__ == "__main__":
    unittest.main()

import asyncio
import os
import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiohttp
import backtrader as bt
import pandas as pd
from aiomoex.client import ISSMoexError

from moex_store import MoexStore
from moex_store.dns_client import DNS_ISSClient


class DummyAsyncResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


class DummyAsyncSession:
    def __init__(self, payload):
        self.payload = payload
        self.urls = []
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        return None

    def get(self, url):
        self.urls.append(url)
        return DummyAsyncResponse(self.payload)


class MoexStoreCacheTests(unittest.TestCase):
    def test_cached_csv_is_loaded_without_remote_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                cache_file = cache_dir / "AFLT_1h_01012023_02012023.csv"
                cache_file.write_text(
                    "datetime,open,high,low,close,volume\n"
                    "2023-01-01 10:00:00,1,2,1,2,100\n"
                    "2023-01-01 11:00:00,2,3,2,3,200\n",
                    encoding="utf-8",
                )

                store = MoexStore(read_from_file=True)

                with patch.object(
                    store,
                    "_validate_inputs",
                    side_effect=AssertionError("remote validation must not run for cached data"),
                ), patch.object(
                    store,
                    "_check_connection",
                    side_effect=AssertionError("connection check must not run for cached data"),
                ), patch.object(
                    store,
                    "_get_candles_history",
                    side_effect=AssertionError("remote candles load must not run for cached data"),
                ):
                    data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

                self.assertIsInstance(data, bt.feeds.PandasData)
                self.assertEqual(len(data.p.dataname), 2)
            finally:
                os.chdir(previous_cwd)

    def test_name_none_is_valid_for_basic_validation(self):
        store = MoexStore()
        store._validate_basic_inputs(
            sec_id="AFLT",
            fromdate=store.validate_fromdate("2023-01-01"),
            todate=store.validate_todate("2023-01-02"),
            tf="1h",
            name=None,
        )

    def test_empty_remote_candles_get_clear_error(self):
        store = MoexStore(write_to_file=False, read_from_file=False)

        with patch.object(store, "_validate_inputs") as validate_inputs, patch.object(
            store,
            "_check_connection",
        ), patch.object(store, "_get_candles_history", return_value=[]):
            validate_inputs.side_effect = lambda sec_id, *_: store.sec_details.update(
                {sec_id: {"market": "shares"}}
            )

            with self.assertRaisesRegex(ValueError, "AFLT"):
                store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

    def test_connection_check_runs_once_per_store_instance(self):
        store = MoexStore(write_to_file=False, read_from_file=False)
        frame = pd.DataFrame(
            {
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [2],
                "volume": [100],
            },
            index=pd.to_datetime(["2023-01-01 10:00:00"]),
        )

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ) as check_connection, patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(return_value=[{"open": 1}]),
        ), patch.object(
            store,
            "_make_df",
            return_value=frame,
        ):
            store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")
            store.getdata("SBER", "2023-01-01", "2023-01-02", "1h")

        self.assertEqual(check_connection.await_count, 1)

    def test_connection_check_failure_does_not_block_direct_candles_load(self):
        store = MoexStore(write_to_file=False, read_from_file=False)
        frame = pd.DataFrame(
            {
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [2],
                "volume": [100],
            },
            index=pd.to_datetime(["2023-01-01 10:00:00"]),
        )

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(side_effect=ConnectionError("engines endpoint unavailable")),
        ) as check_connection, patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(return_value=[{"open": 1}]),
        ) as get_candles_history, patch.object(
            store,
            "_make_df",
            return_value=frame,
        ):
            data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertIsInstance(data, bt.feeds.PandasData)
        self.assertEqual(check_connection.await_count, 1)
        self.assertEqual(get_candles_history.await_count, 1)

    def test_candles_history_is_retried_after_transient_network_error(self):
        store = MoexStore(write_to_file=False, read_from_file=False, max_retries=2, retry_delay=0)
        frame = pd.DataFrame(
            {
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [2],
                "volume": [100],
            },
            index=pd.to_datetime(["2023-01-01 10:00:00"]),
        )

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ), patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(side_effect=[aiohttp.ClientConnectionError("temporary"), [{"open": 1}]]),
        ) as get_candles_history, patch.object(
            store,
            "_make_df",
            return_value=frame,
        ), patch("moex_store.store.timer.sleep") as sleep:
            data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertIsInstance(data, bt.feeds.PandasData)
        self.assertEqual(get_candles_history.await_count, 2)
        sleep.assert_called_once_with(0)

    def test_candles_history_retries_aiomoex_empty_response_attribute_error(self):
        store = MoexStore(write_to_file=False, read_from_file=False, max_retries=2, retry_delay=0)
        frame = pd.DataFrame(
            {
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [2],
                "volume": [100],
            },
            index=pd.to_datetime(["2023-01-01 10:00:00"]),
        )

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ), patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(
                side_effect=[
                    AttributeError("'NoneType' object has no attribute 'get'"),
                    [{"open": 1}],
                ]
            ),
        ) as get_candles_history, patch.object(
            store,
            "_make_df",
            return_value=frame,
        ), patch("moex_store.store.timer.sleep") as sleep:
            data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertIsInstance(data, bt.feeds.PandasData)
        self.assertEqual(get_candles_history.await_count, 2)
        sleep.assert_called_once_with(0)

    def test_non_moex_attribute_error_is_not_retried(self):
        store = MoexStore(write_to_file=False, read_from_file=False, max_retries=2, retry_delay=0)

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ), patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(side_effect=AttributeError("programming error")),
        ) as get_candles_history, patch("moex_store.store.timer.sleep") as sleep:
            with self.assertRaisesRegex(AttributeError, "programming error"):
                store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertEqual(get_candles_history.await_count, 1)
        sleep.assert_not_called()

    def test_candles_history_retry_failure_gets_clear_error(self):
        store = MoexStore(write_to_file=False, read_from_file=False, max_retries=2, retry_delay=0)

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ), patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(side_effect=ISSMoexError("temporary")),
        ) as get_candles_history, patch("moex_store.store.timer.sleep"):
            with self.assertRaisesRegex(ConnectionError, "Не удалось загрузить котировки AFLT за 2"):
                store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertEqual(get_candles_history.await_count, 2)

    def test_candles_history_dns_error_switches_resolver_before_retry(self):
        store = MoexStore(
            write_to_file=False,
            read_from_file=False,
            max_retries=2,
            retry_delay=0,
            dns_servers=("1.1.1.1",),
            dns_policy="system_first",
        )
        frame = pd.DataFrame(
            {
                "open": [1],
                "high": [2],
                "low": [1],
                "close": [2],
                "volume": [100],
            },
            index=pd.to_datetime(["2023-01-01 10:00:00"]),
        )

        def validate_inputs(sec_id, *_):
            store.sec_details[sec_id] = {"market": "shares"}

        with patch.object(store, "_validate_inputs", side_effect=validate_inputs), patch.object(
            store,
            "_check_connection",
            new=AsyncMock(),
        ), patch.object(
            store,
            "_get_candles_history",
            new=AsyncMock(side_effect=[OSError("Timeout while contacting DNS servers"), [{"open": 1}]]),
        ) as get_candles_history, patch.object(
            store,
            "_make_df",
            return_value=frame,
        ):
            data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

        self.assertIsInstance(data, bt.feeds.PandasData)
        self.assertEqual(get_candles_history.await_count, 2)
        self.assertEqual(store._active_dns_servers, ("1.1.1.1",))

    def test_validate_inputs_reuses_existing_security_details(self):
        store = MoexStore(read_from_file=False)
        store.sec_details["EuH3"] = {
            "sectype": "futures",
            "grouptype": "futures_forts",
            "assetcode": "Eu",
            "board": "RFUD",
            "market": "forts",
            "engine": "futures",
        }
        intervals = [
            {
                "begin": "2022-01-01 00:00:00",
                "end": "2024-01-01 00:00:00",
                "interval": 1,
            }
        ]

        with patch.object(
            store,
            "get_instrument_info",
            side_effect=AssertionError("security details must be reused"),
        ), patch.object(
            store,
            "get_history_intervals",
            new=AsyncMock(return_value=intervals),
        ) as get_history_intervals:
            store._validate_inputs(
                "EuH3",
                store.validate_fromdate("2023-01-01"),
                store.validate_todate("2023-01-02"),
                "15m",
                None,
            )

        get_history_intervals.assert_awaited_once_with("EuH3", "RFUD", "forts", "futures")

    def test_validate_inputs_caches_history_intervals(self):
        store = MoexStore(read_from_file=False)
        sec_info = ("futures", "futures_forts", "Eu", "RFUD", "forts", "futures")
        intervals = [
            {
                "begin": "2022-01-01 00:00:00",
                "end": "2024-01-01 00:00:00",
                "interval": 1,
            }
        ]

        with patch.object(
            store,
            "get_instrument_info",
            new=AsyncMock(return_value=sec_info),
        ) as get_instrument_info, patch.object(
            store,
            "get_history_intervals",
            new=AsyncMock(return_value=intervals),
        ) as get_history_intervals:
            store._validate_inputs(
                "EuH3",
                store.validate_fromdate("2023-01-01"),
                store.validate_todate("2023-01-02"),
                "15m",
                None,
            )
            store._validate_inputs(
                "EuH3",
                store.validate_fromdate("2023-01-03"),
                store.validate_todate("2023-01-04"),
                "15m",
                None,
            )

        self.assertEqual(get_instrument_info.await_count, 1)
        self.assertEqual(get_history_intervals.await_count, 1)

    def test_store_metadata_cache_is_loaded_once_per_instance(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                metadata = {
                    "sec_details": {
                        "AFLT": {
                            "sectype": "stock_shares",
                            "grouptype": "stock_shares",
                            "assetcode": None,
                            "board": "TQBR",
                            "market": "shares",
                            "engine": "stock",
                        }
                    }
                }
                (cache_dir / "store_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                store = MoexStore(read_from_file=True)

                self.assertEqual(store._get_cached_sec_details("AFLT")["board"], "TQBR")
                (cache_dir / "store_metadata.json").write_text("{}", encoding="utf-8")

                self.assertEqual(store._get_cached_sec_details("AFLT")["board"], "TQBR")
            finally:
                os.chdir(previous_cwd)

    def test_validate_inputs_loads_security_details_and_intervals_from_disk_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                cache_key = "EuH3|RFUD|forts|futures"
                metadata = {
                    "sec_details": {
                        "EuH3": {
                            "sectype": "futures",
                            "grouptype": "futures_forts",
                            "assetcode": "Eu",
                            "board": "RFUD",
                            "market": "forts",
                            "engine": "futures",
                        }
                    },
                    "history_intervals": {
                        cache_key: [
                            {
                                "begin": "2022-01-01 00:00:00",
                                "end": "2024-01-01 00:00:00",
                                "interval": 1,
                            }
                        ]
                    },
                }
                (cache_dir / "store_metadata.json").write_text(
                    json.dumps(metadata),
                    encoding="utf-8",
                )

                store = MoexStore(read_from_file=True, write_to_file=False)

                with patch.object(
                    store,
                    "get_instrument_info",
                    side_effect=AssertionError("security details must be loaded from disk cache"),
                ), patch.object(
                    store,
                    "get_history_intervals",
                    side_effect=AssertionError("intervals must be loaded from disk cache"),
                ):
                    store._validate_inputs(
                        "EuH3",
                        store.validate_fromdate("2023-01-01"),
                        store.validate_todate("2023-01-02"),
                        "15m",
                        None,
                    )
            finally:
                os.chdir(previous_cwd)

    def test_validate_inputs_skips_interval_check_when_cached_security_details_exist_and_moex_is_unavailable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                metadata = {
                    "sec_details": {
                        "EuH3": {
                            "sectype": "futures",
                            "grouptype": "futures_forts",
                            "assetcode": "Eu",
                            "board": "RFUD",
                            "market": "forts",
                            "engine": "futures",
                        }
                    }
                }
                (cache_dir / "store_metadata.json").write_text(
                    json.dumps(metadata),
                    encoding="utf-8",
                )

                store = MoexStore(read_from_file=True, write_to_file=False)

                with patch.object(
                    store,
                    "get_instrument_info",
                    side_effect=AssertionError("security details must be loaded from disk cache"),
                ), patch.object(
                    store,
                    "_get_history_intervals_with_retries",
                    side_effect=ConnectionError("MOEX unavailable"),
                ), patch("sys.stdout", new_callable=StringIO) as stdout:
                    store._validate_inputs(
                        "EuH3",
                        store.validate_fromdate("2023-01-01"),
                        store.validate_todate("2023-01-02"),
                        "15m",
                        None,
                    )

                self.assertIn("Не удалось проверить интервалы котировок EuH3", stdout.getvalue())
            finally:
                os.chdir(previous_cwd)

    def test_validate_inputs_does_not_skip_interval_check_without_local_security_details(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                store = MoexStore(read_from_file=True, write_to_file=False)
                sec_info = ("futures", "futures_forts", "Eu", "RFUD", "forts", "futures")

                with patch.object(
                    store,
                    "get_instrument_info",
                    new=AsyncMock(return_value=sec_info),
                ), patch.object(
                    store,
                    "_get_history_intervals_with_retries",
                    side_effect=ConnectionError("MOEX unavailable"),
                ):
                    with self.assertRaisesRegex(ConnectionError, "MOEX unavailable"):
                        store._validate_inputs(
                            "EuH3",
                            store.validate_fromdate("2023-01-01"),
                            store.validate_todate("2023-01-02"),
                            "15m",
                            None,
                        )
            finally:
                os.chdir(previous_cwd)

    def test_validate_inputs_writes_security_details_and_intervals_to_disk_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                store = MoexStore(read_from_file=True, write_to_file=True)
                sec_info = ("futures", "futures_forts", "Eu", "RFUD", "forts", "futures")
                intervals = [
                    {
                        "begin": "2022-01-01 00:00:00",
                        "end": "2024-01-01 00:00:00",
                        "interval": 1,
                    }
                ]

                with patch.object(
                    store,
                    "get_instrument_info",
                    new=AsyncMock(return_value=sec_info),
                ), patch.object(
                    store,
                    "get_history_intervals",
                    new=AsyncMock(return_value=intervals),
                ):
                    store._validate_inputs(
                        "EuH3",
                        store.validate_fromdate("2023-01-01"),
                        store.validate_todate("2023-01-02"),
                        "15m",
                        None,
                    )

                cache = json.loads(Path("files_from_moex/store_metadata.json").read_text(encoding="utf-8"))
                self.assertEqual(cache["sec_details"]["EuH3"]["board"], "RFUD")
                self.assertEqual(cache["history_intervals"]["EuH3|RFUD|forts|futures"], intervals)
            finally:
                os.chdir(previous_cwd)

    def test_broken_csv_cache_falls_back_to_remote_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                cache_file = cache_dir / "AFLT_1h_01012023_02012023.csv"
                cache_file.write_text(
                    "datetime,open,high,low,close\n"
                    "2023-01-01 10:00:00,1,2,1,2\n",
                    encoding="utf-8",
                )
                store = MoexStore(read_from_file=True, write_to_file=False)
                frame = pd.DataFrame(
                    {
                        "open": [1],
                        "high": [2],
                        "low": [1],
                        "close": [2],
                        "volume": [100],
                    },
                    index=pd.to_datetime(["2023-01-01 10:00:00"]),
                )

                def validate_inputs(sec_id, *_):
                    store.sec_details[sec_id] = {"market": "shares"}

                with patch.object(store, "_validate_inputs", side_effect=validate_inputs) as validate_inputs_mock, patch.object(
                    store,
                    "_check_connection",
                    new=AsyncMock(),
                ), patch.object(
                    store,
                    "_get_candles_history",
                    new=AsyncMock(return_value=[{"open": 1}]),
                ) as get_candles_history, patch.object(
                    store,
                    "_make_df",
                    return_value=frame,
                ):
                    data = store.getdata("AFLT", "2023-01-01", "2023-01-02", "1h")

                self.assertIsInstance(data, bt.feeds.PandasData)
                validate_inputs_mock.assert_called_once()
                self.assertEqual(get_candles_history.await_count, 1)
            finally:
                os.chdir(previous_cwd)

    def test_history_intervals_falls_back_to_board_endpoint_when_market_endpoint_fails(self):
        store = MoexStore(read_from_file=False)
        session = DummyAsyncSession({})
        intervals = [
            {
                "begin": "2022-01-01 00:00:00",
                "end": "2024-01-01 00:00:00",
                "interval": 1,
            }
        ]

        with patch.object(store, "_create_session", return_value=session), patch(
            "moex_store.store.aiomoex.get_market_candle_borders",
            new=AsyncMock(side_effect=ISSMoexError("market unavailable")),
        ) as market_borders, patch(
            "moex_store.store.aiomoex.get_board_candle_borders",
            new=AsyncMock(return_value=intervals),
        ) as board_borders:
            result = asyncio.run(store.get_history_intervals("EuH3", "RFUD", "forts", "futures"))

        self.assertEqual(result, intervals)
        market_borders.assert_awaited_once()
        board_borders.assert_awaited_once()

    def test_moex_store_does_not_wrap_aiohttp_globally(self):
        session_init = aiohttp.ClientSession.__init__
        connector_init = aiohttp.TCPConnector.__init__

        MoexStore(dns_servers=("1.1.1.1",), request_timeout=11)
        MoexStore(dns_servers=("8.8.8.8",), request_timeout=22)

        self.assertIs(aiohttp.ClientSession.__init__, session_init)
        self.assertIs(aiohttp.TCPConnector.__init__, connector_init)

    def test_apply_ssl_patch_is_compatibility_noop(self):
        store = MoexStore()
        session_init = aiohttp.ClientSession.__init__
        connector_init = aiohttp.TCPConnector.__init__

        store.apply_ssl_patch()

        self.assertIs(aiohttp.ClientSession.__init__, session_init)
        self.assertIs(aiohttp.TCPConnector.__init__, connector_init)

    def test_dns_servers_can_be_configured_explicitly(self):
        store = MoexStore(dns_servers=("1.2.3.4", "5.6.7.8"))

        self.assertEqual(store.dns_servers, ("1.2.3.4", "5.6.7.8"))

    def test_dns_servers_can_be_configured_from_env(self):
        with patch.dict(os.environ, {"MOEX_STORE_DNS_SERVERS": "9.9.9.9, 1.1.1.1"}):
            store = MoexStore()

        self.assertEqual(store.dns_servers, ("9.9.9.9", "1.1.1.1"))

    def test_default_dns_policy_starts_with_system_dns(self):
        store = MoexStore(dns_servers=("1.1.1.1",))

        self.assertEqual(store.network_policy, "auto")
        self.assertEqual(store.dns_policy, "system_first")
        self.assertIsNone(store._active_dns_servers)

    def test_custom_first_dns_policy_starts_with_custom_dns(self):
        store = MoexStore(dns_servers=("1.1.1.1",), dns_policy="custom_first")

        self.assertEqual(store._active_dns_servers, ("1.1.1.1",))

    def test_invalid_network_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "network_policy"):
            MoexStore(network_policy="unknown")

    def test_invalid_log_level_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "log_level"):
            MoexStore(log_level="verbose")

    def test_log_level_silent_suppresses_network_diagnostics(self):
        store = MoexStore(
            dns_servers=("1.1.1.1",),
            network_policy="auto",
            dns_policy="system_first",
            log_level="silent",
        )

        with patch("sys.stdout", new_callable=StringIO) as stdout:
            switched = store._try_dns_fallback(OSError("Timeout while contacting DNS servers"))

        self.assertTrue(switched)
        self.assertEqual(stdout.getvalue(), "")

    def test_log_level_info_prints_dns_fallback_diagnostic(self):
        store = MoexStore(
            dns_servers=("1.1.1.1",),
            network_policy="auto",
            dns_policy="system_first",
            log_level="info",
        )

        with patch("sys.stdout", new_callable=StringIO) as stdout:
            switched = store._try_dns_fallback(OSError("Timeout while contacting DNS servers"))

        self.assertTrue(switched)
        self.assertIn("Переключаю resolver", stdout.getvalue())

    def test_log_level_debug_prints_session_resolver(self):
        store = MoexStore(dns_servers=("1.1.1.1",), dns_policy="custom_first", log_level="debug")

        with patch.object(store, "_create_connector", return_value=object()), patch.object(
            store,
            "_create_timeout",
            return_value=object(),
        ), patch("moex_store.store.aiohttp.ClientSession"), patch(
            "sys.stdout",
            new_callable=StringIO,
        ) as stdout:
            store._create_session()

        self.assertIn("Создаю MOEX session: resolver=1.1.1.1", stdout.getvalue())

    def test_log_level_debug_prints_proxy_and_trust_env(self):
        store = MoexStore(proxy="http://127.0.0.1:8080", trust_env=True, log_level="debug")

        with patch.object(store, "_create_connector", return_value=object()), patch.object(
            store,
            "_create_timeout",
            return_value=object(),
        ), patch("moex_store.store.aiohttp.ClientSession"), patch(
            "sys.stdout",
            new_callable=StringIO,
        ) as stdout:
            store._create_session()

        self.assertIn("proxy=http://127.0.0.1:8080", stdout.getvalue())
        self.assertIn("trust_env=True", stdout.getvalue())

    def test_dns_error_switches_from_system_to_custom_dns_in_auto_policy(self):
        store = MoexStore(dns_servers=("1.1.1.1",), network_policy="auto", dns_policy="system_first")

        switched = store._try_dns_fallback(OSError("Timeout while contacting DNS servers"))

        self.assertTrue(switched)
        self.assertEqual(store._active_dns_servers, ("1.1.1.1",))

    def test_dns_error_switches_from_custom_to_system_dns_in_auto_policy(self):
        store = MoexStore(dns_servers=("1.1.1.1",), network_policy="auto", dns_policy="custom_first")

        switched = store._try_dns_fallback(OSError("Timeout while contacting DNS servers"))

        self.assertTrue(switched)
        self.assertIsNone(store._active_dns_servers)

    def test_strict_network_policy_does_not_switch_dns(self):
        store = MoexStore(dns_servers=("1.1.1.1",), network_policy="strict", dns_policy="system_first")

        switched = store._try_dns_fallback(OSError("Timeout while contacting DNS servers"))

        self.assertFalse(switched)
        self.assertIsNone(store._active_dns_servers)

    def test_non_dns_error_does_not_switch_dns(self):
        store = MoexStore(dns_servers=("1.1.1.1",), network_policy="auto", dns_policy="system_first")

        switched = store._try_dns_fallback(aiohttp.ServerDisconnectedError("server disconnected"))

        self.assertFalse(switched)
        self.assertIsNone(store._active_dns_servers)

    def test_create_timeout_uses_instance_settings(self):
        store = MoexStore(
            request_timeout=11,
            connect_timeout=3,
            sock_read_timeout=7,
        )

        timeout = store._create_timeout()

        self.assertEqual(timeout.total, 11)
        self.assertEqual(timeout.connect, 3)
        self.assertEqual(timeout.sock_connect, 3)
        self.assertEqual(timeout.sock_read, 7)

    def test_create_connector_uses_system_dns_when_active_dns_is_none(self):
        store = MoexStore()

        with patch("moex_store.store.aiohttp.TCPConnector") as connector, patch(
            "moex_store.store.aiohttp.resolver.AsyncResolver",
        ) as async_resolver:
            result = store._create_connector()

        self.assertIs(result, connector.return_value)
        async_resolver.assert_not_called()
        self.assertNotIn("resolver", connector.call_args.kwargs)

    def test_create_connector_uses_custom_dns_when_active_dns_is_set(self):
        store = MoexStore(dns_servers=("1.1.1.1",), dns_policy="custom_first")

        with patch("moex_store.store.aiohttp.TCPConnector") as connector, patch(
            "moex_store.store.aiohttp.resolver.AsyncResolver",
        ) as async_resolver:
            result = store._create_connector()

        self.assertIs(result, connector.return_value)
        async_resolver.assert_called_once_with(nameservers=["1.1.1.1"])
        self.assertIs(connector.call_args.kwargs["resolver"], async_resolver.return_value)

    def test_create_session_uses_instance_connector_and_timeout(self):
        store = MoexStore()
        connector = object()
        timeout = object()

        with patch.object(store, "_create_connector", return_value=connector), patch.object(
            store,
            "_create_timeout",
            return_value=timeout,
        ), patch("moex_store.store.aiohttp.ClientSession") as client_session:
            result = store._create_session()

        self.assertIs(result, client_session.return_value)
        client_session.assert_called_once_with(connector=connector, timeout=timeout, trust_env=False)

    def test_create_session_uses_explicit_proxy(self):
        store = MoexStore(proxy="http://127.0.0.1:8080")
        connector = object()
        timeout = object()

        with patch.object(store, "_create_connector", return_value=connector), patch.object(
            store,
            "_create_timeout",
            return_value=timeout,
        ), patch("moex_store.store.aiohttp.ClientSession") as client_session:
            result = store._create_session()

        self.assertIs(result, client_session.return_value)
        client_session.assert_called_once_with(
            connector=connector,
            timeout=timeout,
            trust_env=False,
            proxy="http://127.0.0.1:8080",
        )

    def test_create_session_can_trust_environment_proxy_settings(self):
        store = MoexStore(trust_env=True)
        connector = object()
        timeout = object()

        with patch.object(store, "_create_connector", return_value=connector), patch.object(
            store,
            "_create_timeout",
            return_value=timeout,
        ), patch("moex_store.store.aiohttp.ClientSession") as client_session:
            result = store._create_session()

        self.assertIs(result, client_session.return_value)
        client_session.assert_called_once_with(connector=connector, timeout=timeout, trust_env=True)

    def test_moex_store_configures_dns_client_retries(self):
        MoexStore(max_retries=5, retry_delay=0.25)

        self.assertEqual(DNS_ISSClient.retry_attempts, 5)
        self.assertEqual(DNS_ISSClient.retry_delay, 0.25)

    def test_make_df_writes_csv_atomically(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_file = os.path.join(tmpdir, "quotes.csv")
            store = MoexStore()
            rows = [
                {
                    "begin": "2023-01-01 10:00:00",
                    "open": 1,
                    "high": 2,
                    "low": 1,
                    "close": 2,
                    "value": 10,
                    "volume": 100,
                }
            ]

            df = store._make_df(rows, "shares", csv_file)

            self.assertTrue(os.path.exists(csv_file))
            self.assertFalse(os.path.exists(f"{csv_file}.tmp"))
            self.assertEqual(len(df), 1)

    def test_atomic_write_dataframe_leaves_old_file_when_csv_write_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_file = os.path.join(tmpdir, "quotes.csv")
            Path(csv_file).write_text("old-data", encoding="utf-8")
            frame = pd.DataFrame({"open": [1]})

            with patch.object(
                pd.DataFrame,
                "to_csv",
                side_effect=OSError("disk full"),
            ):
                with self.assertRaises(OSError):
                    MoexStore._atomic_write_dataframe(frame, csv_file)

            self.assertEqual(Path(csv_file).read_text(encoding="utf-8"), "old-data")

    def test_futures_expiration_dates_can_be_loaded_from_metadata_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                metadata = {
                    "history_stat": {
                        "Eu": [
                            ["EuM3", "Eu-6.23", "2023-03-16", "2023-06-15", "Eu", "EUR/RUB", 0],
                            ["EuH3", "Eu-3.23", "2022-12-15", "2023-03-16", "Eu", "EUR/RUB", 0],
                        ]
                    }
                }
                (cache_dir / "futures_metadata.json").write_text(
                    json.dumps(metadata),
                    encoding="utf-8",
                )

                store = MoexStore(read_from_file=True)

                with patch.object(
                    store.futures,
                    "get_asset_code",
                    side_effect=AssertionError("MOEX lookup must not run for cached futures metadata"),
                ):
                    self.assertEqual(store.futures.expdate("EuM3"), "2023-06-15")
                    self.assertEqual(store.futures.prevexpdate("EuM3"), "2023-03-16")
            finally:
                os.chdir(previous_cwd)

    def test_futures_metadata_is_sorted_and_deduplicated_for_previous_expiration(self):
        history_stat = [
            ["EuH8_2018", "Eu-3.18", "2016-11-22", "2018-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuM8", "Eu-6.18", "2018-03-15", "2018-06-21", "Eu", "EUR_RUB__TOM", 0],
            ["EuH8", "EuH8", "2016-11-22", "2018-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ7", "Eu-12.17", "2017-09-21", "2017-12-21", "Eu", "EUR_RUB__TOM", 0],
        ]

        normalized = MoexStore().futures._normalize_history_stat(history_stat)

        self.assertEqual([item[0] for item in normalized], ["EuM8", "EuH8", "EuZ7"])

    def test_futures_metadata_prefers_regular_contract_over_calendar_spread(self):
        history_stat = [
            ["EuZ6EuH7", "EuZ6EuH7", "2026-03-13", "2026-12-17", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ6", "Eu-12.26", "2025-06-13", "2026-12-17", "Eu", "EUR_RUB__TOM", 1],
            ["EuU6EuZ6", "EuU6EuZ6", "2025-12-12", "2026-09-17", "Eu", "EUR_RUB__TOM", 0],
            ["EuU6", "Eu-9.26", "2025-03-14", "2026-09-17", "Eu", "EUR_RUB__TOM", 1],
        ]

        normalized = MoexStore().futures._normalize_history_stat(history_stat)

        self.assertEqual([item[0] for item in normalized], ["EuZ6", "EuU6"])

    def test_futures_expiration_alias_is_found_in_metadata_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                metadata = {
                    "history_stat": {
                        "Eu": [
                            ["EuH0_2000", "EuH0_2000", "2009-06-03", "2010-03-15", "Eu", "EUR_RUB__TOM", 0],
                            ["EuH0_2010", "Eu-3.10", "2010-01-11", "2010-03-15", "Eu", "EUR_RUB__TOM", 0],
                            ["EuZ9_2009", "Eu-12.09", "2009-03-06", "2009-12-15", "Eu", "EUR_RUB__TOM", 0],
                        ]
                    }
                }
                (cache_dir / "futures_metadata.json").write_text(
                    json.dumps(metadata),
                    encoding="utf-8",
                )

                store = MoexStore(read_from_file=True)

                with patch.object(
                    store.futures,
                    "get_asset_code",
                    side_effect=AssertionError("MOEX lookup must not run for cached futures metadata"),
                ):
                    self.assertEqual(store.futures.expdate("EuH0_2010"), "2010-03-15")
                    self.assertEqual(store.futures.prevexpdate("EuH0_2010"), "2009-12-15")
            finally:
                os.chdir(previous_cwd)

    def test_futures_expiration_reuses_loaded_history_stat(self):
        store = MoexStore(read_from_file=False, write_to_file=False)
        store.sec_details["EuM3"] = {
            "sectype": "futures",
            "grouptype": "futures_forts",
            "assetcode": "Eu",
            "board": "RFUD",
            "market": "forts",
            "engine": "futures",
        }
        stat_payload = {
            "series": {
                "data": [
                    ["EuM3", "Eu-6.23", "2023-03-16", "2023-06-15", "Eu", "EUR_RUB__TOM", 0],
                    ["EuH3", "Eu-3.23", "2022-12-15", "2023-03-16", "Eu", "EUR_RUB__TOM", 0],
                ]
            }
        }

        with patch.object(
            store.futures,
            "_get_futures_stat",
            new=AsyncMock(return_value=stat_payload),
        ) as get_futures_stat:
            self.assertEqual(store.futures.prevexpdate("EuM3"), "2023-03-16")
            self.assertEqual(store.futures.expdate("EuM3"), "2023-06-15")
            self.assertEqual(store.futures.get_history_stat("Eu", to_active=False, show_table=False)[0][0], "EuM3")

        self.assertEqual(get_futures_stat.await_count, 1)

    def test_futures_history_stat_uses_metadata_cache_without_moex_refresh(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                cache_dir = Path("files_from_moex")
                cache_dir.mkdir()
                metadata = {
                    "history_stat": {
                        "Eu": [
                            ["EuM3", "Eu-6.23", "2023-03-16", "2023-06-15", "Eu", "EUR_RUB__TOM", 0],
                            ["EuH3", "Eu-3.23", "2022-12-15", "2023-03-16", "Eu", "EUR_RUB__TOM", 0],
                        ]
                    }
                }
                (cache_dir / "futures_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                store = MoexStore(read_from_file=True, write_to_file=False)

                with patch.object(
                    store.futures,
                    "_get_futures_stat",
                    new=AsyncMock(side_effect=AssertionError("MOEX refresh must not run when cache exists")),
                ):
                    result = store.futures._get_or_fetch_history_stat("Eu")

                self.assertEqual(result, metadata["history_stat"]["Eu"])
            finally:
                os.chdir(previous_cwd)

    def test_futures_sec_info_reuses_parent_security_details(self):
        store = MoexStore(read_from_file=False)
        store.sec_details["EuM3"] = {
            "sectype": "futures",
            "grouptype": "futures_forts",
            "assetcode": "Eu",
            "board": "RFUD",
            "market": "forts",
            "engine": "futures",
        }

        with patch.object(
            store,
            "get_instrument_info",
            side_effect=AssertionError("parent security details must be reused"),
        ):
            self.assertEqual(store.futures.get_asset_code("EuM3"), "Eu")

    def test_futures_sec_info_uses_parent_retry_wrapper(self):
        store = MoexStore(read_from_file=False, write_to_file=False)
        sec_info = ("futures", "futures_forts", "Eu", "RFUD", "forts", "futures")

        with patch.object(
            store,
            "_get_instrument_info_with_retries",
            return_value=sec_info,
        ) as get_instrument_info_with_retries, patch.object(
            store,
            "get_instrument_info",
            side_effect=AssertionError("direct MOEX lookup must go through retry wrapper"),
        ):
            self.assertEqual(store.futures.get_asset_code("EuM3"), "Eu")

        get_instrument_info_with_retries.assert_called_once_with("EuM3")

    def test_futures_sec_info_populates_store_metadata_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                store = MoexStore(read_from_file=True, write_to_file=True)
                sec_info = ("futures", "futures_forts", "Eu", "RFUD", "forts", "futures")

                with patch.object(
                    store,
                    "get_instrument_info",
                    new=AsyncMock(return_value=sec_info),
                ):
                    self.assertEqual(store.futures.get_asset_code("EuM3"), "Eu")

                store_cache = json.loads(Path("files_from_moex/store_metadata.json").read_text(encoding="utf-8"))
                futures_cache = json.loads(Path("files_from_moex/futures_metadata.json").read_text(encoding="utf-8"))
                self.assertEqual(store_cache["sec_details"]["EuM3"]["assetcode"], "Eu")
                self.assertEqual(futures_cache["sec_info"]["EuM3"]["assetcode"], "Eu")
            finally:
                os.chdir(previous_cwd)

    def test_futures_stat_uses_parent_session_factory(self):
        store = MoexStore()
        session = DummyAsyncSession({"series": {"data": []}})

        with patch.object(store, "_create_session", return_value=session) as create_session:
            result = asyncio.run(store.futures._get_futures_stat("Eu"))

        self.assertEqual(result, {"series": {"data": []}})
        create_session.assert_called_once()
        self.assertTrue(session.entered)
        self.assertTrue(session.exited)
        self.assertEqual(len(session.urls), 1)
        self.assertIn("asset_code=Eu", session.urls[0])

    def test_futures_stat_retries_transient_network_error(self):
        store = MoexStore(max_retries=2, retry_delay=0)
        session = DummyAsyncSession({})

        with patch.object(store, "_create_session", return_value=session), patch.object(
            store.futures,
            "_fetch_json_once",
            new=AsyncMock(side_effect=[aiohttp.ClientConnectionError("temporary"), {"series": {"data": []}}]),
        ) as fetch_json_once, patch("moex_store.futures.asyncio.sleep", new=AsyncMock()) as sleep:
            result = asyncio.run(store.futures._get_futures_stat("Eu"))

        self.assertEqual(result, {"series": {"data": []}})
        self.assertEqual(fetch_json_once.await_count, 2)
        sleep.assert_awaited_once_with(0)

    def test_futures_stat_dns_error_switches_resolver_before_retry(self):
        store = MoexStore(max_retries=2, retry_delay=0, dns_servers=("1.1.1.1",), dns_policy="system_first")
        session = DummyAsyncSession({})

        with patch.object(store, "_create_session", return_value=session), patch.object(
            store.futures,
            "_fetch_json_once",
            new=AsyncMock(side_effect=[OSError("Timeout while contacting DNS servers"), {"series": {"data": []}}]),
        ) as fetch_json_once:
            result = asyncio.run(store.futures._get_futures_stat("Eu"))

        self.assertEqual(result, {"series": {"data": []}})
        self.assertEqual(fetch_json_once.await_count, 2)
        self.assertEqual(store._active_dns_servers, ("1.1.1.1",))

    def test_futures_stat_empty_response_is_retryable(self):
        store = MoexStore(max_retries=2, retry_delay=0)
        session = DummyAsyncSession({})

        with patch.object(store, "_create_session", return_value=session), patch.object(
            store.futures,
            "_fetch_json_once",
            new=AsyncMock(side_effect=[aiohttp.ClientPayloadError("empty"), {"series": {"data": []}}]),
        ) as fetch_json_once, patch("moex_store.futures.asyncio.sleep", new=AsyncMock()):
            result = asyncio.run(store.futures._get_futures_stat("Eu"))

        self.assertEqual(result, {"series": {"data": []}})
        self.assertEqual(fetch_json_once.await_count, 2)

    def test_futures_stat_reuses_passed_session(self):
        store = MoexStore()
        session = DummyAsyncSession({"series": {"data": []}})

        with patch.object(
            store,
            "_create_session",
            side_effect=AssertionError("passed session must be reused"),
        ):
            result = asyncio.run(store.futures._get_futures_stat("Eu", session=session))

        self.assertEqual(result, {"series": {"data": []}})
        self.assertFalse(session.entered)
        self.assertFalse(session.exited)
        self.assertEqual(len(session.urls), 1)

    def test_futures_info_uses_parent_session_factory(self):
        store = MoexStore()
        session = DummyAsyncSession({"description": {"data": []}})

        with patch.object(store, "_create_session", return_value=session) as create_session:
            result = asyncio.run(store.futures._get_futures_info("EuH6"))

        self.assertEqual(result, {"description": {"data": []}})
        create_session.assert_called_once()
        self.assertIn("/iss/securities/EuH6.json", session.urls[0])

    def test_all_futures_uses_parent_session_factory(self):
        store = MoexStore()
        session = DummyAsyncSession({"securities": {"data": []}})

        with patch.object(store, "_create_session", return_value=session) as create_session:
            result = asyncio.run(store.futures._get_all_futures())

        self.assertEqual(result, {"securities": {"data": []}})
        create_session.assert_called_once()
        self.assertIn("/iss/engines/futures/markets/forts/securities.json", session.urls[0])


if __name__ == "__main__":
    unittest.main()

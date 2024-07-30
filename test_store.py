import unittest
from unittest.mock import patch
import aiohttp
import asyncio
from aioresponses import aioresponses
from moex_store.store import MoexStore

class TestMoexStore(unittest.TestCase):

    def test_check_connection_success(self):
        with aioresponses() as m:
            m.get('https://iss.moex.com/iss/engines.xml', status=200)

            store = MoexStore(write_to_file=True)
            self.assertTrue(store.wtf)

    def test_check_connection_failure_status(self):
        with aioresponses() as m:
            m.get('https://iss.moex.com/iss/engines.xml', status=500)

            with self.assertRaises(ConnectionError):
                MoexStore(write_to_file=True)

    def test_check_connection_failure_client_error(self):
        with patch('aiohttp.ClientSession.get', side_effect=aiohttp.ClientError):
            with self.assertRaises(ConnectionError):
                MoexStore(write_to_file=True)

    def test_check_connection_failure_timeout(self):
        with patch('aiohttp.ClientSession.get', side_effect=asyncio.TimeoutError):
            with self.assertRaises(ConnectionError):
                MoexStore(write_to_file=True)

if __name__ == '__main__':
    unittest.main()

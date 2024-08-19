import unittest
from unittest.mock import patch, AsyncMock
import asyncio
from datetime import datetime
from moex_store.store import MoexStore


class TestMoexStore(unittest.TestCase):

    def setUp(self):
        # Создание нового цикла событий перед каждым тестом
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # Создание экземпляра MoexStore перед каждым тестом
        self.store = MoexStore(write_to_file=False)

    def tearDown(self):
        # Закрытие цикла событий после каждого теста
        self.loop.close()

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    def test_get_data_success(self, mock_get):
        # Мокируем успешный ответ для _check_connection
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value={'key': 'value'})

        # Мокируем успешные ответы для get_instrument_info и get_history_intervals
        with patch('moex_store.store.MoexStore.get_instrument_info',
                   new=AsyncMock(return_value=('TQBR', 'shares', 'stock'))):
            with patch('moex_store.store.MoexStore.get_history_intervals', new=AsyncMock(
                    return_value=[{'interval': 1, 'begin': '2000-01-01 00:00:00', 'end': '2024-12-31 00:00:00'}])):
                with patch('aiomoex.get_market_candles', new=AsyncMock(return_value=[
                    {'begin': '2023-01-01 00:00:00', 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1}])):
                    fromdate = datetime(2023, 1, 1)
                    todate = datetime(2023, 1, 10)
                    data = self.store.get_data('test', 'SBER', fromdate, todate, '1m')
                    self.assertIsNotNone(data)

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    def test_get_data_invalid_date(self, mock_get):
        # Мокируем успешный ответ для _check_connection
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value={'key': 'value'})

        fromdate = datetime(2023, 1, 10)
        todate = datetime(2023, 1, 1)
        with self.assertRaises(ValueError):
            self.store.get_data('test', 'SBER', fromdate, todate, '1m')

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    def test_get_data_invalid_tf(self, mock_get):
        # Мокируем успешный ответ для _check_connection
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value={'key': 'value'})

        fromdate = datetime(2023, 1, 1)
        todate = datetime(2023, 1, 10)
        with self.assertRaises(ValueError):
            self.store.get_data('test', 'SBER', fromdate, todate, '2h')

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    def test_get_data_instrument_not_found(self, mock_get):
        # Мокируем успешный ответ для _check_connection
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value={'key': 'value'})

        with patch('moex_store.store.MoexStore.get_instrument_info', new=AsyncMock(return_value=None)):
            fromdate = datetime(2023, 1, 1)
            todate = datetime(2023, 1, 10)
            with self.assertRaises(ValueError):
                self.store.get_data('test', 'INVALID', fromdate, todate, '1m')


if __name__ == '__main__':
    unittest.main()

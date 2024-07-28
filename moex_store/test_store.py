import unittest
import sys
import os
from datetime import datetime
import pandas as pd
import backtrader as bt
import asyncio

# Добавляем путь к директории moex_store
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from moex_store.store import MoexStore

class TestMoexStore(unittest.TestCase):

    def setUp(self):
        self.store = MoexStore(write_to_file=False)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_check_connection(self):
        result = self.loop.run_until_complete(self.store._check_connection())
        self.assertIsNone(result)

    def test_get_data(self):
        # Проверка получения данных
        data = self.store.get_data('test', 'AFLT', '2024-03-15', '2024-05-29', '1d')
        self.assertIsInstance(data, bt.feeds.PandasData)

    def test_parse_date(self):
        # Проверка парсинга даты
        date_str = '2024-03-15'
        parsed_date = self.store._parse_date(date_str)
        self.assertEqual(parsed_date, datetime(2024, 3, 15))

        date_dt = datetime(2024, 3, 15)
        parsed_date = self.store._parse_date(date_dt)
        self.assertEqual(parsed_date, date_dt)

    def test_validate_inputs(self):
        # Проверка валидации входных параметров
        fromdate = datetime(2024, 3, 15)
        todate = datetime(2024, 5, 29)
        self.store._validate_inputs('AFLT', fromdate, todate, '1d')
        # если не возникло исключений, то тест пройден

    def test_make_df(self):
        # Проверка создания DataFrame
        data = [
            {'begin': '2024-03-15 00:00:00', 'open': 100, 'high': 110, 'low': 90, 'close': 105, 'volume': 1000},
            {'begin': '2024-03-16 00:00:00', 'open': 105, 'high': 115, 'low': 95, 'close': 110, 'volume': 1100}
        ]
        tf = '1d'
        market = 'shares'
        df = self.store.make_df(data, tf, market)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 5))  # 2 строки, 5 столбцов

if __name__ == '__main__':
    unittest.main()

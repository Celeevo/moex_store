from collections import OrderedDict
import os
import time
import pandas as pd
import backtrader as bt
import aiohttp
import asyncio
import aiomoex
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
from moex_store.tf import change_tf

# import patch_aiohttp


TF = OrderedDict({
    '1m': 1,
    '5m': 5,
    '10m': 10,
    '15m': 15,
    '30m': 30,
    '1h': 60,
    '1d': 24,
    '1w': 7,
    '1M': 31,
    '1q': 4
})


class MoexStore:
    def __init__(self, write_to_file=True):
        # Проверка соединения
        self.wtf = write_to_file
        asyncio.run(self._check_connection())

    async def _check_connection(self):
        url = "https://iss.moex.com"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        print("Биржа MOEX доступна для запросов")
                    else:
                        raise ConnectionError(f"Не удалось подключиться к MOEX: статус {response.status}")
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к MOEX: {e}")

    def get_data(self, name, sec_id, fromdate, todate, tf):
        fromdate = self._parse_date(fromdate)
        todate = self._parse_date(todate)

        # Проверка значений
        self._validate_inputs(sec_id, fromdate, todate, tf)

        # Получение данных
        moex_data = asyncio.run(self._get_candles_history(sec_id, fromdate, todate, TF[tf]))
        # Готовим итоговый пандас дата-фрейм для backtrader.cerebro
        moex_df = self.make_df(moex_data, tf, self.market)  # формируем файл с историей
        data = bt.feeds.PandasData(name=name, dataname=moex_df)
        return data

    def _parse_date(self, date_input):
        if isinstance(date_input, datetime):
            return date_input
        elif isinstance(date_input, str):
            for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
                try:
                    return datetime.strptime(date_input, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Неверный формат даты: {date_input}. Используйте тип datetime или тип "
                             f"str в форматах 'YYYY-MM-DD' и 'DD-MM-YYYY'.")
        else:
            raise ValueError(f"Дата должна быть типа datetime или str, получили {type(date_input).__name__}")

    def _validate_inputs(self, sec_id, fromdate, todate, tf):
        # Проверка fromdate <= todate
        if fromdate > todate:
            raise ValueError(f"fromdate ({fromdate}) должен быть меньше (раньше) или равен todate ({todate})")

        # Проверка наличия tf в TF
        if tf not in TF:
            raise ValueError(
                f"Тайм-фрейм для {sec_id} должен быть одним из списка: {list(TF.keys())}, получили: {tf = }")

        # Проверка get_instrument_info
        instrument_info = asyncio.run(self.get_instrument_info(sec_id))
        if instrument_info is None:
            raise ValueError(f"Инструмент с sec_id {sec_id} не найден на Бирже")
        print(f'Инструмент {sec_id} найден на Бирже')
        self.board, self.market, self.engine = instrument_info
        if self.wtf:
            self.sec_id = sec_id

        # Проверка get_history_intervals
        interval_data = asyncio.run(self.get_history_intervals(sec_id, self.board, self.market, self.engine))
        if interval_data is None:
            raise ValueError(f"Нет доступных интервалов для sec_id {sec_id}")

        valid_interval = None
        for interval in interval_data:
            # Если запрошен тайм-фрейм 5, 15 или 30 мин, то проверяем наличие на Биржи котировок
            # с тайм-фреймом 1 мин, так как из них будут приготовлены котировки для 5, 15 или 30 мин.
            tff = 1 if TF[tf] in (5, 15, 30) else TF[tf]
            if interval['interval'] == tff:
                valid_interval = interval
                break

        if not valid_interval:
            raise ValueError(f"Тайм-фрейм {tf} не доступен для инструмента {sec_id}")

        valid_begin = datetime.strptime(valid_interval['begin'], '%Y-%m-%d %H:%M:%S')
        valid_end = datetime.strptime(valid_interval['end'], '%Y-%m-%d %H:%M:%S')

        if not (valid_begin <= fromdate <= valid_end):
            raise ValueError(f"fromdate ({fromdate}) должен быть между {valid_begin} и {valid_end}")

    async def get_instrument_info(self, secid):
        async with aiohttp.ClientSession() as session:
            url = f"https://iss.moex.com/iss/securities/{secid}.json"
            async with session.get(url) as response:
                data = await response.json()

                if 'boards' in data and 'data' in data['boards'] and data['boards']['data']:
                    boards_data = data['boards']['data']
                    columns = data['boards']['columns']

                    primary_boards = filter(lambda item: dict(zip(columns, item)).get('is_primary') == 1, boards_data)

                    for item in primary_boards:
                        record = dict(zip(columns, item))
                        board = record.get('boardid')
                        market = record.get('market')
                        engine = record.get('engine')
                        return board, market, engine
                else:
                    return None

    async def get_history_intervals(self, sec_id, board, market, engine):
        async with aiohttp.ClientSession() as session:
            data = await aiomoex.get_market_candle_borders(session, security=sec_id, market=market, engine=engine)
            if data:
                return data

            data = await aiomoex.get_board_candle_borders(session, security=sec_id, board=board, market=market,
                                                          engine=engine)
            if data:
                return data

            return None

    async def _get_candles_history(self, sec_id, fromdate, todate, tf):
        start = fromdate.strftime('%Y-%m-%d %H:%M:%S')
        end = todate.strftime('%Y-%m-%d %H:%M:%S')

        if tf in (5, 15, 30):
            resample_tf_value = tf
            tf = 1
        else:
            resample_tf_value = None

        async with aiohttp.ClientSession() as session:
            estimated_time = self.get_estimated_time(fromdate, todate, tf)
            # pbar_task = asyncio.create_task(self._run_progress_bar(estimated_time))
            # data_task = aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end,
            #                                        market=self.market, engine=self.engine)

            data_task = asyncio.create_task(
                aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end, market=self.market,
                                           engine=self.engine))
            pbar_task = asyncio.create_task(self._run_progress_bar(estimated_time, data_task))

            start_time = time.time()
            data = await data_task
            end_time = time.time()
            elapsed_time = end_time - start_time

            await pbar_task

            # data = await aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end, market=self.market, engine=self.engine)
            if data:
                print(f'История котировок для {sec_id} получена с {tf = } за {elapsed_time:.2f} секунды')
            else:
                data = await aiomoex.get_board_candles(session, sec_id, interval=tf, start=start, end=end, board=self.board, market=self.market, engine=self.engine)
                if data:
                    print(f'История котировок для {sec_id} получена с {tf = }')
                else:
                    print(f'История котировок для {sec_id} с {tf = } не найдена на бирже')
                    return None

            if resample_tf_value:
                tf = resample_tf_value
                print(f'Пересчитываю ТФ для {sec_id} c 1 мин на {tf} мин')
                data = change_tf(data, tf)

            return data

    def make_df(self, data, tf, market):
        df = pd.DataFrame(data)
        if market == 'index':
            if 'volume' in df.columns:
                df.drop(columns=['volume'], inplace=True)
            df.rename(columns={'value': 'volume'}, inplace=True)  # VOLUME = value, ибо Индексы имеют только value
        else:
            if 'value' in df.columns:
                df.drop(columns=['value'], inplace=True)
        df.rename(columns={'begin': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'])  # Преобразование в datetime
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        df.set_index('datetime', inplace=True)

        if self.wtf:
            csv_file_path = f"files_from_moex/{self.sec_id}_tf-{tf}.csv"
            directory = os.path.dirname(csv_file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            df.to_csv(csv_file_path, sep=',', index=True, header=True)
            print(f'Котировки для {self.sec_id} записаны в файл "{csv_file_path}"')

        return df

    def get_estimated_time(self, fromdate, todate, tf):
        delta_days = (todate - fromdate).days
        delta_months = delta_days / 30
        if tf in [1, 5, 15, 30]:
            a, b, c = 0.45, 6.68, 1.59
        elif tf == 10:
            a, b, c = 0.07, 0.43, 1.5
        elif tf in [60, 24, 7, 31, 4]:
            a, b, c = 0.1, 0.4, 0.9
        return a * delta_months ** 2 + b * delta_months + c

    async def _run_progress_bar(self, duration, data_task):
        with tqdm_asyncio(total=100, desc="Fetching market candles", leave=True, ncols=100,
                          bar_format='{l_bar}{bar}') as pbar:
            for _ in range(100):
                if data_task.done():
                    pbar.n = 100
                    pbar.refresh()
                    break
                await asyncio.sleep(duration / 100)
                pbar.update(1)




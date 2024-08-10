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
import moex_store.patch_aiohttp
import ssl
from aiohttp.client_exceptions import ClientConnectorCertificateError
from ssl import SSLCertVerificationError


TF = {'1m': 1, '5m': 5, '10m': 10, '15m': 15, '30m': 30, '1h': 60, '1d': 24, '1w': 7, '1M': 31, '1q': 4}


class MoexStore:
    def __init__(self, write_to_file=True, max_retries=3, retry_delay=2):
        self.wtf = write_to_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ssl_context = None
        asyncio.run(self._check_connection())

    def apply_ssl_patch(self):
        # Создаем SSL-контекст с отключенной проверкой сертификатов
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        # Переопределяем оригинальный метод ClientSession
        _original_init = aiohttp.ClientSession.__init__

        def _patched_init(self, *args, **kwargs):
            if 'connector' not in kwargs:
                kwargs['connector'] = aiohttp.TCPConnector(ssl=self.ssl_context)
            _original_init(self, *args, **kwargs)

        aiohttp.ClientSession.__init__ = _patched_init

    async def _check_connection(self):
        url = f"https://iss.moex.com/iss/engines.json"
        attempts = 0
        ssl_patched = False

        while attempts < self.max_retries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        _ = await response.json()
                        if response.status == 200:
                            print("Биржа MOEX доступна для запросов")
                            return
                        else:
                            raise ConnectionError(f"Не удалось подключиться к MOEX: статус {response.status}")
            except (ClientConnectorCertificateError, SSLCertVerificationError) as e:
                if not ssl_patched:
                    print(f"SSL verification failed: {e}")
                    print(f'Похоже вы запускаете приложение на Мак ОС, но не воспользовались рекомендацией по '
                          f'установке сертификатов при инсталляции Python, типа: "Congratulations! Python 3.9.0 '
                          f'for macOS 10.9 or later  was successfully installed. One more thing: to verify '
                          f'the identity of secure network connections, this Python needs a set of SSL root '
                          f'certificates. You can download and install a current curated set from the Certifi '
                          f'project by double-clicking on the Install Certificates icon in the Finder window. '
                          f'See the ReadMe file for more information."')
                    print("Ищите и запускайте файл 'Install Certificates.command' в папке Python 3.XX")

                    self.apply_ssl_patch()
                    ssl_patched = True  # патч применен
                else:
                    print(f"Попытка {attempts + 1} с отключенной проверкой SSL не удалась: {e}")
                    attempts += 1
                    if attempts < self.max_retries:
                        time.sleep(self.retry_delay)
            except aiohttp.ClientError as e:
                print(f"Попытка {attempts + 1}: Не удалось подключиться к MOEX: {e}")
                attempts += 1
                if attempts < self.max_retries:
                    time.sleep(self.retry_delay)
            except Exception as e:
                raise ConnectionError(f"Не удалось подключиться к MOEX: {e}")

    def get_data(self, name, sec_id, fromdate, todate, tf):
        fromdate = self._parse_date(fromdate)
        todate = self._parse_date(todate)

        # Проверка значений
        self._validate_inputs(sec_id, fromdate, todate, tf)

        # Получение данных
        moex_data = asyncio.run(self._get_candles_history(sec_id, fromdate, todate, tf))
        # Готовим итоговый дата-фрейм для backtrader.cerebro
        moex_df = self.make_df(moex_data, tf, self.market)  # формируем файл с историей
        data = bt.feeds.PandasData(name=name, dataname=moex_df)
        return data

    @staticmethod
    def _parse_date(date_input):
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

    @staticmethod
    async def get_instrument_info(secid):
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

    @staticmethod
    async def get_history_intervals(sec_id, board, market, engine):
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
        delta = (todate - fromdate).days
        start = fromdate.strftime('%Y-%m-%d %H:%M:%S')
        end = todate.strftime('%Y-%m-%d %H:%M:%S')
        key_tf = tf
        tf = TF[tf]

        if tf in (5, 15, 30):
            resample_tf_value = tf
            tf = 1
        else:
            resample_tf_value = None

        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            if tf in (1, 10, 60, 24):
                estimated_time = self.get_estimated_time(delta, tf)
                print(f'Ожидаемое время загрузки данных (зависит от загрузки серверов MOEX): {estimated_time:.0f} сек.')
                time.sleep(0.1)
                data_task = asyncio.create_task(
                    aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end, market=self.market,
                                               engine=self.engine))
                pbar_task = asyncio.create_task(
                    self._run_progress_bar(estimated_time, data_task))

                data = await data_task

                await pbar_task
            else:
                print(f'Загружаю котировки ...')
                data = await aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end,
                                                        market=self.market, engine=self.engine)

            # with open("output.txt", "a") as file:
            #     file.write(f'{tf = }, {delta = }, elapsed_time = {elapsed_time:.2f}' + "\n")

            end_time = time.time()
            elapsed_time = end_time - start_time
            if data:
                print(f'История котировок {sec_id} c {fromdate.strftime("%Y-%m-%d")} по {todate.strftime("%Y-%m-%d")} '
                      f'на тайм-фрейме "{key_tf}" получена за {elapsed_time:.2f} секунды')
            else:
                data = await aiomoex.get_board_candles(session, sec_id, interval=tf, start=start, end=end,
                                                       board=self.board, market=self.market, engine=self.engine)
                if data:
                    print(f'История котировок для {sec_id} получена с тайм-фреймом {key_tf}')
                else:
                    print(f'История котировок для {sec_id} с тайм-фреймом  {key_tf} не найдена на бирже')
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
            print(f'Котировки записаны в файл "{csv_file_path}"')

        return df

    @staticmethod
    def get_estimated_time(delta, tf):
        a, b, c = 0, 0, 0
        if tf == 1:
            a, b, c = 0.0003295019925172705, 0.04689869997675399, 6.337785868761401
        elif tf == 10:
            a, b, c = 4.988531246349836e-06, 0.012451095862652674, 0.48478245834903433
        elif tf in [60, 24]:
            a, b, c = - 1.4234264995077613e-07, 0.0024511947309111748, 0.5573157754716476
        return a * delta ** 2 + b * delta + c

    @staticmethod
    async def _run_progress_bar(duration, data_task):
        with tqdm_asyncio(total=100, desc="Загружаю котировки", leave=True, ncols=100,
                          bar_format='{l_bar}{bar}') as pbar:
            for _ in range(100):
                if data_task.done():
                    pbar.n = 100
                    pbar.refresh()
                    break
                await asyncio.sleep(duration / 100)
                pbar.update(1)

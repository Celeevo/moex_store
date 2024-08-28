import os
import time
from pprint import pprint

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
        self.sec_details = {}
        # self.ssl_context = None
        asyncio.run(self._check_connection())

    def apply_ssl_patch(self):
        # Создаем SSL-контекст с отключенной проверкой сертификатов
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Переопределяем оригинальный метод ClientSession
        _original_init = aiohttp.ClientSession.__init__

        def _patched_init(self, *args, **kwargs):
            if 'connector' not in kwargs:
                kwargs['connector'] = aiohttp.TCPConnector(ssl=ssl_context)
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
                          f'for macOS 10.9 or later was successfully installed. One more thing: to verify '
                          f'the identity of secure network connections, this Python needs a set of SSL root '
                          f'certificates. You can download and install a current curated set from the Certifi '
                          f'project by double-clicking on the Install Certificates icon in the Finder window. '
                          f'See the ReadMe file for more information."')
                    print("Ищите и запускайте файл 'Install Certificates.command' в папке Python 3.XX. Пока пробую "
                          "отключить проверку сертификатов.")

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

    # def get_data(self, name, sec_id, fromdate, todate, tf):
    def get_data(self, sec_id, fromdate, todate, tf='1h', name=None):
        fromdate = self._parse_date(fromdate)
        todate = self._parse_date(todate)

        # Проверка значений
        self._validate_inputs(sec_id, fromdate, todate, tf, name)

        # Получение исторических котировок
        moex_data = asyncio.run(self._get_candles_history(sec_id, fromdate, todate, tf))
        # Готовим итоговый дата-фрейм для backtrader.cerebro
        moex_df = self.make_df(moex_data, tf, self.sec_details[sec_id]['market'], sec_id)  # формируем файл с историей
        data = bt.feeds.PandasData(dataname=moex_df, fromdate=fromdate, todate=todate, name=name)

        return data

    getdata = get_data

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
                             f"str в формате 'YYYY-MM-DD' или 'DD-MM-YYYY'.")
        else:
            raise ValueError(f"Дата должна быть типа datetime или str, получили {type(date_input).__name__}")

    def _validate_inputs(self, sec_id, fromdate, todate, tf, name):
        if not isinstance(name, str):
           raise ValueError(f"Тип имени источника данных должен быть str, получен {type(name).__name__}")
        # Проверка fromdate <= todate
        if fromdate >= todate:
            raise ValueError(f"fromdate ({fromdate}) должен быть меньше (раньше) todate ({todate}), \n"
                             f"для получения котировок за один день используйте, например для 2023-06-20: \n"
                             f"fromdate = '2023-06-20', todate = '2023-06-21'")

        # Проверка наличия tf в TF
        if tf not in TF:
            raise ValueError(
                f"Тайм-фрейм для {sec_id} должен быть одним из списка: {list(TF.keys())}, получен: {tf = }")

        # Проверка get_instrument_info
        sec_info = asyncio.run(self.get_instrument_info(sec_id))

        if sec_info[-1] is None:
            raise ValueError(f"Инструмент с sec_id {sec_id} не найден на Бирже")

        print(f'Инструмент {sec_id} найден на Бирже')
        self.sec_details[sec_id] = dict(
            sectype=sec_info[0],
            grouptype=sec_info[1],
            assetcode=sec_info[2],
            board=sec_info[3],
            market=sec_info[4],
            engine=sec_info[5]
        )
        # pprint(self.sec_details[sec_id])

        # Проверка доступных интервалов котировок get_history_intervals
        interval_data = asyncio.run(self.get_history_intervals(sec_id, self.sec_details[sec_id]['board'],
                                                               self.sec_details[sec_id]['market'],
                                                               self.sec_details[sec_id]['engine']))

        # [{'begin': '2022-05-18 11:45:00', 'end': '2024-03-21 13:59:00', 'interval': 1,
        # 'board_group_id': 45} {'begin': '2022-04-01 00:00:00', 'end': '2024-01-01 00:00:00', 'interval': 4,
        # 'board_group_id': 45}, ... ]

        if interval_data is None:
            raise ValueError(f"На Бирже нет доступных интервалов котировок для инструмента {sec_id}.")

        # Если запрошен тайм-фрейм 5, 15 или 30 мин, то проверяем наличие на Биржи котировок
        # с тайм-фреймом 1 мин, так как из них будут приготовлены котировки для 5, 15 или 30 мин.
        user_tf = 1 if TF[tf] in (5, 15, 30) else TF[tf]
        valid_interval = next((item for item in interval_data if item['interval'] == user_tf), None)
        # print(f'{valid_interval}')

        if not valid_interval:
            raise ValueError(f"Тайм-фрейм {tf} не доступен для инструмента {sec_id}")

        valid_begin = datetime.strptime(valid_interval['begin'], '%Y-%m-%d %H:%M:%S')
        valid_end = datetime.strptime(valid_interval['end'], '%Y-%m-%d %H:%M:%S')

        if fromdate > valid_end:
            # raise ValueError(f"fromdate ({fromdate}) для {sec_id} должен быть между {valid_begin} и {valid_end}")
            raise ValueError(f"fromdate ({fromdate}) для {sec_id} и тайм-фрейма '{tf}' должен быть меньше (раньше) {valid_end}, \n"
                             f"валидный интервал с {valid_interval['begin']} по {valid_interval['end']}")

        if todate < valid_begin:
            raise ValueError(f"todate ({todate}) для {sec_id} и тайм-фрейма '{tf}' должен быть больше (позже) {valid_begin}, \n"
                             f"валидный интервал с {valid_interval['begin']} по {valid_interval['end']}")


    # @staticmethod
    async def get_instrument_info(self, secid):
        async with aiohttp.ClientSession() as session:
            url = f"https://iss.moex.com/iss/securities/{secid}.json"
            # https://iss.moex.com/iss/securities/GZU4.json
            # https://iss.moex.com/iss/engines/futures/markets/forts/securities/RIU4.json
            # https://iss.moex.com/iss/statistics/engines/futures/markets/forts/series.json?asset_code=rts&show_expired=1
            async with session.get(url) as response:
                data = await response.json()

                sectype, grouptype, assetcode, board, market, engine = None, None, None, None, None, None

                if 'description' in data and 'data' in data['description'] and data['description']['data']:
                    description_dict = {item[0]: item[2] for item in data['description']['data']}
                    sectype = description_dict.get("TYPE")
                    grouptype = description_dict.get("GROUPTYPE")
                    assetcode = description_dict.get("ASSETCODE")  # if sectype == "futures" else None

                if 'boards' in data and 'data' in data['boards'] and data['boards']['data']:
                    boards_data = data['boards']['data']
                    columns = data['boards']['columns']

                    # Ищем в data['boards']['data'] строку с is_primary = 1 (это главная доска инструмента)
                    primary_boards = filter(lambda item: dict(zip(columns, item)).get('is_primary') == 1, boards_data)

                    for item in primary_boards:
                        record = dict(zip(columns, item))
                        board = record.get('boardid')
                        market = record.get('market')
                        engine = record.get('engine')

                return sectype, grouptype, assetcode, board, market, engine

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
                    aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end,
                                               market=self.sec_details[sec_id]['market'],
                                               engine=self.sec_details[sec_id]['engine']))
                pbar_task = asyncio.create_task(
                    self._run_progress_bar(estimated_time, data_task))

                data = await data_task

                await pbar_task
            else:
                print(f'Загружаю котировки ...')
                data = await aiomoex.get_market_candles(session, sec_id, interval=tf, start=start, end=end,
                                                        market=self.sec_details[sec_id]['market'],
                                                        engine=self.sec_details[sec_id]['engine'])

            end_time = time.time()
            elapsed_time = end_time - start_time
            if data:
                print(f'История котировок {sec_id} c {fromdate.strftime("%Y-%m-%d")} по {todate.strftime("%Y-%m-%d")} '
                      f'на тайм-фрейме "{key_tf}" получена за {elapsed_time:.2f} секунды')
            else:
                data = await aiomoex.get_board_candles(session, sec_id, interval=tf, start=start, end=end,
                                                       board=self.sec_details[sec_id]['board'],
                                                       market=self.sec_details[sec_id]['market'],
                                                       engine=self.sec_details[sec_id]['engine'])
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

    def make_df(self, data, tf, market, sec_id):
        df = pd.DataFrame(data)
        # print(df.columns)

        # Определим необходимые столбцы
        required_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin']

        # Проверим наличие всех необходимых столбцов
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Отсутствуют необходимые столбцы: {', '.join(missing_columns)}")

        if market == 'index':
            df.drop(columns=['volume'], inplace=True)
            df.rename(columns={'value': 'volume'}, inplace=True)  # VOLUME = value, ибо Индексы имеют только value
        else:
            df.drop(columns=['value'], inplace=True)

        df.rename(columns={'begin': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'])  # Преобразование в datetime
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        df.set_index('datetime', inplace=True)

        if self.wtf:
            csv_file_path = f"files_from_moex/{sec_id}_tf-{tf}.csv"
            directory = os.path.dirname(csv_file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            try:
                df.to_csv(csv_file_path, sep=',', index=True, header=True)
                print(f'Котировки записаны в файл "{csv_file_path}"')
            except IOError as e:
                print(f"Ошибка при записи файла: {e}")

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
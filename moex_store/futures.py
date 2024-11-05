import os
import webbrowser
# from threading import active_count

import aiohttp
import asyncio
from datetime import datetime, timedelta
import logging
import pandas as pd


class MOEXConnectionError(Exception):
    """Исключение для ошибок подключения к MOEX."""
    pass

class Futures:
    def __init__(self, parent):
        self.parent = parent  # Ссылка на родительский класс, если нужно

    def get_sec_info(self, sec_id):
        '''
        Возвращает sectype, grouptype, assetcode, board, market, engine.
        Записывает данные в словарь self.parent.sec_details
        :param sec_id: str Код инструмента
        :return: dict с ключами sectype, grouptype, assetcode, board, market, engine
        '''
        if not isinstance(sec_id, str):
            print(f"Тикер sec_id должен быть строкой, получен тип: {type(sec_id).__name__}")
            return None
        sec_info = asyncio.run(self.parent.get_instrument_info(sec_id))

        if sec_info[-1] is None:
            raise ValueError(f"Инструмент {sec_id = } не найден на Бирже")

        # print(f'Инструмент {sec_id} найден на Бирже')
        self.parent.sec_details[sec_id] = dict(
            sectype=sec_info[0],
            grouptype=sec_info[1],
            assetcode=sec_info[2],
            board=sec_info[3],
            market=sec_info[4],
            engine=sec_info[5]
        )

        if self.parent.sec_details[sec_id]['sectype'] != 'futures':
            raise ValueError(f'Вызвана функция для фьючерса, но {sec_id} - это не фьючерс. Тип инструмента: '
                             f'{self.parent.sec_details[sec_id]["sectype"]}. Проверьте ваши инструменты.')

        return self.parent.sec_details[sec_id]

    def get_asset_code(self, sec_id):
        '''
        Возвращает Код базового актива (SECID) для кода фьючерсного контракта
        :param sec_id: str, код фьючерсного контракта
        :return: str, ASSETCODE базового актива
        '''
        sec_info = self.get_sec_info(sec_id)
        if sec_info and sec_info['sectype'] == 'futures':
            return sec_info['assetcode']

        return None

    def get_active_contract(self, asset, date=datetime.today()):
        if not isinstance(asset, str):
            print(f"Код базового актива должен быть строкой, получен тип: {type(asset).__name__}")
            return None
        stat = self.get_history_stat(asset, to_active=False, show_table=False)
        return self._get_active_contract(stat, date=date)

    def get_history_list(self, asset, to_active=True):
        if not isinstance(asset, str):
            print(f"Код базового актива должен быть строкой, получен тип: {type(asset).__name__}")
            return None
        stat = self.get_history_stat(asset, to_active=to_active, show_table=False)
        if stat:
            history_list = [item[0] for item in stat]
            history_list.reverse()
            return history_list

        return []

    def get_n_last_contracts(self, asset, n=4, to_active=True):
        if not isinstance(asset, str):
            print(f"Код базового актива должен быть строкой, получен тип: {type(asset).__name__}")
            return None
        if not isinstance(n, int) or n < 0:
            print(f"Значение n ({n}) должен быть целым, положительным числом. Получен неверный тип ({type(n).__name__}) или отрицательное число")
            return None
        clist = self.get_history_list(asset, to_active=to_active)
        n_last_contracts = clist[-n:]
        return n_last_contracts

    def get_contracts_between(self, asset, from_date=datetime.today()-timedelta(days=365), to_date=datetime.today()):
        if not isinstance(asset, str):
            print(f"Код базового актива должен быть строкой, получен тип: {type(asset).__name__}")
            return None
        from_date = self.parent.validate_fromdate(from_date)
        to_date = self.parent.validate_todate(to_date)
        if to_date < from_date:
            print(f"Дата to_date {to_date.date()} должна быть больше или равна from_date {from_date.date()}. Получили {to_date < from_date = }")
            return None
        stat = self.get_history_stat(asset, to_active=False, show_table=False)
        contract_list = self.get_history_list(asset, to_active=False)
        from_date_contract = self._get_active_contract(stat, from_date)
        to_date_contract = self._get_active_contract(stat, to_date)
        if not to_date_contract and from_date_contract and from_date_contract in contract_list: # Дата to_date позж активных контартов
            c_list = contract_list[contract_list.index(from_date_contract):]
            return c_list
        if from_date_contract not in contract_list or to_date_contract not in contract_list:
            print("Не найдено активных контрактов на заданные даты начала/конца интервала.")
            return None
        # Получаем индексы контрактов
        index1 = contract_list.index(from_date_contract)
        index2 = contract_list.index(to_date_contract)

        # Проверяем, что первый контракт раньше второго
        if index1 > index2:
            print("Первый контракт (from_date) находится после второго (to_date) в списке.")
            return None

        # Возвращаем срез списка от второго до первого контракта включительно
        c_list = contract_list[index1:index2 + 1]
        return c_list

    def get_contract_exp_date(self, sec_id):
        asset = self.get_asset_code(sec_id)
        if asset:
            stat = self.get_history_stat(asset, to_active=False, show_table=False)
            if stat:
                exp_date = next((item[3] for item in stat if item[0] == sec_id), None)
                if not exp_date:
                    return None

                return exp_date

    def get_previous_contract_exp_date(self, sec_id):
        asset = self.get_asset_code(sec_id)
        if asset:
            stat = self.get_history_stat(asset, to_active=False, show_table=False)
            if stat:
                index = next((i for i, item in enumerate(stat) if item[0] == sec_id), None)
                if index is None or index + 1 >= len(stat):
                    return None
                return stat[index + 1][3]

    def get_history_stat(self, asset, to_active=True, show_table=True):
        stat = asyncio.run(self._get_futures_stat(asset))
        valid_return = stat.get('series', {}).get('data', [])
        # Биржа отдает в качестве ответа список списков, каждый вложенный список имеет вид:
        # ['secid', 'name', 'start_date', 'expiration_date', 'asset_code', 'underlying_asset', 'is_traded']
        # Возвращаю список, у которого вложенные списки будут такие же, но при условии, что 'start_date' не
        # равно None, 'expiration_date' не равно None, и 'name' содержит свои первые 2 символа только один раз
        # (например если 'name' = "SiZ4SiH5", в список не добавляю).
        # if 'series' in stat and 'data' in stat['series'] and stat['series']['data']:

        if valid_return:
            history_stat = [item for item in stat['series']['data']
                            if item[2] is not None and item[3] is not None and
                            item[1].count(item[1][:2]) == 1]
            if to_active:
                active_contract = self._get_active_contract(history_stat)
                history_list = [item[0] for item in history_stat]
                if active_contract and history_list and (active_contract in history_list):
                    target_index = history_list.index(active_contract)
                    history_stat = history_stat[target_index:]
            if show_table:
                columns = ["secid", "shortname", "startdate", "expdate", "assetcode", "underlyingasset",
                           "is_traded"]
                self._display_table(history_stat, columns, name="exp_dates")
                # return None
            else:
                return history_stat
        print(f"Биржа не вернула статистику по коду базового актива {asset}. Проверьте его.")
        return None

    def get_all_active_futures(self, show_table=True):
        '''
        Биржа возвращает в качестве ответа список списков, каждый вложенный список имеет вид:
        ["SECID", "BOARDID", "SHORTNAME", "SECNAME", "PREVSETTLEPRICE", "DECIMALS", "MINSTEP", "LASTTRADEDATE",
        "LASTDELDATE", "SECTYPE", "LATNAME", "ASSETCODE", "PREVOPENPOSITION", "LOTVOLUME", "INITIALMARGIN",
        "HIGHLIMIT", "LOWLIMIT", "STEPPRICE", "LASTSETTLEPRICE", "PREVPRICE", "IMTIME", "BUYSELLFEE", "SCALPERFEE",
        "NEGOTIATEDFEE", "EXERCISEFEE"]

        :param show_table: True - сохранять результат в файле html и открывать его в браузере и ничего не возвращать, 
        или (False) вернуть результат в виде списка списков

        :return: html-файл или список списков
        '''

        stat = asyncio.run(self._get_all_futures())
        valid_return = stat.get('securities', {}).get('data', [])
        if valid_return:
            idx = (0,2,11,7,5,6,17,13,14)
            # active_futures = [[self._to_number(item[id]) for id in idx] for item in stat['securities']['data']]
            active_futures = [[item[id] for id in idx] for item in stat['securities']['data']]

            for futures in active_futures:
                info = asyncio.run(self._get_futures_info(futures[0]))
                '''
                url = f"https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
                "description": {
                    "columns": ["name", "title", "value", "type", "sort_order", "is_hidden", "precision"],
                    "data": [
                        ["SECID", "Краткий код", "SiU3", "string", 1, 0, null],
                        ["NAME", "Наименование серии инструмента", "Фьючерсный контракт Si-9.23", "string", 2, 0, null],
                        ["SHORTNAME", "Краткое наименование контракта", "Si-9.23", "string", 3, 0, null],
                        ["LATNAME", "Английское наименование", "Si-9.23", "string", 4, 1, null],
                        ["DELIVERYTYPE", "Исполнение", "В качестве цены исполнения принимается значение <a href=https:\/\/www.moex.com\/ru\/markets\/currency\/get-fixing.aspx?code=USDFIXME>фиксинга USDFIXME<\/a>, рассчитываемого в соответствии с <a href=http:\/\/fs.moex.com\/files\/3971\/>Методикой<\/a>, умноженное на количество долларов США в Лоте и округленное с точностью до целых по правилам математического округления.", "string", 5, 0, null],
                        ["FRSTTRADE", "Начало обращения", "2021-09-24", "date", 9, 0, null],
                        ["LSTTRADE", "Последний день обращения", "2023-09-21", "date", 10, 0, null],
                        ["LSTDELDATE", "Дата исполнения", "2023-09-21", "date", 11, 0, null],
                        ["ASSETCODE", "Код базового актива", "Si", "string", 20, 0, null],
                        ["EXECTYPE", "Тип контракта", "Расчетный", "string", 21, 0, null],
                        ["LOTSIZE", "Лот", "1000", "number", 22, 0, 0],
                        ["CONTRACTNAME", "Наименование контракта", "Фьючерсный контракт на курс доллар США - российский рубль", "string", 23, 0, null],
                        ["GROUPTYPE", "Группа контрактов", "Валюта", "string", 24, 0, null],
                        ["UNIT", "Котировка", "в рублях за лот", "string", 25, 0, null],
                        ["TYPENAME", "Вид контракта", "Фьючерс", "string", 9000, 0, null],
                        ["GROUP", "Код типа инструмента", "futures_forts", "string", 9001, 1, null],
                        ["TYPE", "Тип бумаги", "futures", "string", 10000, 1, null],
                        ["GROUPNAME", "Типа инструмента", "Фьючерсы", "string", 10011, 1, null]
                '''
                valid_return = info.get('description', {}).get('data', [])
                if valid_return:
                    info = {item[0]: item[2] for item in info['description']['data']}
                    futures.insert(3, info["CONTRACTNAME"])
                    futures.insert(4, info["GROUPTYPE"])
                    futures.insert(5, info["FRSTTRADE"])

            if show_table:
                columns = ["SECID", "SHORTNAME", "ASSETCODE", "CONTRACTNAME", "GROUPTYPE", "FRSTTRADE",
                           "LASTTRADEDATE", "DECIMALS", "MINSTEP", "STEPPRICE", "LOTVOLUME", "INITIALMARGIN"]
                self._display_table(active_futures, columns, "active_futures")
                return None
            else:
                return active_futures
        print(f"Биржа не вернула информацию")
        return None

    @staticmethod
    async def _get_all_futures(session=None):
        url = f"https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off&nearest=1"
        try:
            async with session or aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()  # Автоматически выбрасывает исключение при ошибке
                    data = await response.json()

                    if not data or 'securities' not in data:
                        raise ValueError(f"Пустой ответ или отсутствуют данные")

                    return data

        except aiohttp.ClientError as e:
            logging.error(f"Ошибка подключения к MOEX: {e}")
            raise MOEXConnectionError(f"Не удалось подключиться к MOEX: {e}")
        except Exception as e:
            logging.error(f"Ошибка получения данных: {e}")
            raise MOEXConnectionError(f"Не удалось получить данные: {e}")

    @staticmethod
    async def _get_futures_info(secid):
            url = f"https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off&iss.only=description"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        data = await response.json()

                        if not data or 'description' not in data:
                            raise ValueError(f"Пустой ответ или отсутствуют данные")

                        return data

            except aiohttp.ClientError as e:
                logging.error(f"Ошибка подключения к MOEX: {e}")
                raise MOEXConnectionError(f"Не удалось подключиться к MOEX: {e}")
            except Exception as e:
                logging.error(f"Ошибка получения данных: {e}")
                raise MOEXConnectionError(f"Не удалось получить данные: {e}")

    @staticmethod
    async def _get_futures_stat(asset, session=None):
        url = f"https://iss.moex.com/iss/statistics/engines/futures/markets/forts/series.json?iss.meta=off&asset_code={asset}&show_expired=1"
        try:
            async with session or aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()  # Автоматически выбрасывает исключение при ошибке
                    data = await response.json()

                    if not data or 'series' not in data:
                        raise ValueError(f"Пустой ответ или отсутствуют данные для актива {asset}")

                    return data

        except aiohttp.ClientError as e:
            logging.error(f"Ошибка подключения к MOEX: {e}")
            raise MOEXConnectionError(f"Не удалось подключиться к MOEX: {e}")
        except Exception as e:
            logging.error(f"Ошибка получения статистики для {asset}: {e}")
            raise MOEXConnectionError(f"Не удалось получить статистику по базовому активу {asset} : {e}")

    def _get_active_contract(self, stat, date=datetime.today()):
        if stat:
            dstat = [item[:2] + [datetime.strptime(item[2], "%Y-%m-%d").date()] +
                         [datetime.strptime(item[3], "%Y-%m-%d").date()] + item[4:]
                         for item in stat]

            data = self.parent.validate_todate(date)
            if isinstance(data, datetime):
                data = data.date()
            filtered = [item for item in dstat if item[3] > data]
            if not filtered:
                print(f"Не нашли активных фьючерсных контрактов на дату {data}")
                return None
                # return dstat[0][0]
            closest_item = min(filtered, key=lambda x: x[3] - data)
            return closest_item[0]
        return None

    def _display_table(self, data, columns, name):
        # Создание и отображение таблицы
        df = pd.DataFrame(data, columns=columns).fillna("-")
        styled = df.style.set_properties(**{'text-align': 'center', 'border': '1px solid black'}).set_table_styles(
            [{'selector': 'thead th', 'props': [('border', '1px solid black')]}]
            ).format(formatter=self._to_number).hide(axis="index")

        # Сохранение в HTML
        html_file_path = os.path.abspath(f"{name}.html")
        styled.to_html(html_file_path)
        # df.to_html(html_file_path, index=False, border=1, justify='center')

        # Открытие HTML в браузере
        webbrowser.open(f"file://{html_file_path}")

    @staticmethod
    def _to_number(value):
        try:
            number = float(value)
            if number.is_integer():
                return int(number)
            return number
        except (ValueError, TypeError):
            return value


    asset = get_asset_code
    info = get_sec_info
    stat = get_history_stat
    list = get_history_list
    active = get_active_contract
    nlast = get_n_last_contracts
    expdate = get_contract_exp_date
    prevexpdate = get_previous_contract_exp_date
    all_active = get_all_active_futures
    contracts_between = get_contracts_between
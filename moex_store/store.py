import json
import os
import socket
import sys
import time as timer
from pprint import pprint

import pandas as pd
import backtrader as bt
import aiohttp
import asyncio
import aiomoex
from datetime import datetime, time
from tqdm.asyncio import tqdm_asyncio
from moex_store.tf import change_tf
import moex_store.patch_aiohttp
from moex_store.dns_client import DNS_ISSClient
import ssl
from aiohttp.client_exceptions import ClientConnectorCertificateError
from aiomoex.client import ISSMoexError
from ssl import SSLCertVerificationError
from moex_store.futures import Futures
import certifi

TF = {'1m': 1, '5m': 5, '10m': 10, '15m': 15, '30m': 30, '1h': 60, '1d': 24, '1w': 7, '1M': 31, '1q': 4}
DEFAULT_DNS_SERVERS = ("77.88.8.8", "77.88.8.1", "1.1.1.1", "8.8.8.8")
NETWORK_POLICIES = ("auto", "interactive", "strict")
DNS_POLICIES = ("system_first", "custom_first", "system_only", "custom_only")
LOG_LEVELS = ("silent", "error", "info", "debug")
LOG_LEVEL_PRIORITY = {"silent": 0, "error": 1, "info": 2, "debug": 3}
_AIOHTTP_SSL_PATCHED = False

class MoexStore:
    def __init__(
            self,
            write_to_file=True,
            read_from_file=True,
            max_retries=3,
            retry_delay=2,
            dns_servers=None,
            force_ipv4=True,
            request_timeout=60,
            connect_timeout=10,
            sock_read_timeout=30,
            network_policy="auto",
            dns_policy="system_first",
            log_level="info",
            proxy=None,
            trust_env=False,
    ):
        self.network_policy = self._validate_option("network_policy", network_policy, NETWORK_POLICIES)
        self.dns_policy = self._validate_option("dns_policy", dns_policy, DNS_POLICIES)
        self.log_level = self._validate_option("log_level", log_level, LOG_LEVELS)
        self.proxy = proxy
        self.trust_env = trust_env
        self.dns_servers = self._resolve_dns_servers(dns_servers)
        self._active_dns_servers = self._initial_dns_servers()
        self.force_ipv4 = force_ipv4
        self.request_timeout = request_timeout
        self.connect_timeout = connect_timeout
        self.sock_read_timeout = sock_read_timeout
        self.wtf = write_to_file
        self.rff = read_from_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        DNS_ISSClient.retry_attempts = max(1, self.max_retries)
        DNS_ISSClient.retry_delay = self.retry_delay
        self._connection_checked = False
        self._metadata_cache = None
        self.sec_details = {}
        self._history_intervals_cache = {}
        self.futures = Futures(self)

    @staticmethod
    def _validate_option(name, value, allowed_values):
        if value not in allowed_values:
            raise ValueError(f"{name} должен быть одним из {allowed_values}, получен: {value}")
        return value

    def _log(self, level, message):
        if LOG_LEVEL_PRIORITY[level] <= LOG_LEVEL_PRIORITY[self.log_level]:
            print(message)

    @staticmethod
    def _resolve_dns_servers(dns_servers=None):
        if dns_servers:
            return tuple(dns_servers)

        env_dns_servers = os.environ.get("MOEX_STORE_DNS_SERVERS")
        if env_dns_servers:
            return tuple(item.strip() for item in env_dns_servers.split(",") if item.strip())

        return DEFAULT_DNS_SERVERS

    def _initial_dns_servers(self):
        if self.dns_policy in ("custom_first", "custom_only"):
            return self.dns_servers
        return None

    def _create_ssl_context(self):
        return ssl.create_default_context(cafile=certifi.where())

    def _create_timeout(self):
        return aiohttp.ClientTimeout(
            total=self.request_timeout,
            connect=self.connect_timeout,
            sock_connect=self.connect_timeout,
            sock_read=self.sock_read_timeout,
        )

    def _create_connector(self):
        family = socket.AF_INET if self.force_ipv4 else socket.AF_UNSPEC
        connector_kwargs = dict(
            ssl=self._create_ssl_context(),
            family=family,
            ttl_dns_cache=300,
        )
        if self._active_dns_servers is not None:
            connector_kwargs["resolver"] = aiohttp.resolver.AsyncResolver(nameservers=list(self._active_dns_servers))
        return aiohttp.TCPConnector(**connector_kwargs)

    def _create_session(self):
        self._log(
            "debug",
            f"Создаю MOEX session: resolver={self._format_dns_servers(self._active_dns_servers)}, "
            f"proxy={self.proxy or 'none'}, trust_env={self.trust_env}"
        )
        session_kwargs = dict(
            connector=self._create_connector(),
            timeout=self._create_timeout(),
            trust_env=self.trust_env,
        )
        if self.proxy:
            session_kwargs["proxy"] = self.proxy
        return aiohttp.ClientSession(**session_kwargs)

    @staticmethod
    def _iter_exception_chain(error):
        current = error
        seen = set()
        while current and id(current) not in seen:
            seen.add(id(current))
            yield current
            current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)

    @classmethod
    def _is_dns_error(cls, error):
        dns_error_type = getattr(aiohttp.client_exceptions, "ClientConnectorDNSError", None)
        dns_markers = (
            "dns",
            "getaddrinfo",
            "cannot resolve host",
            "timeout while contacting dns servers",
            "name or service not known",
            "nodename nor servname",
            "temporary failure in name resolution",
        )
        for item in cls._iter_exception_chain(error):
            if dns_error_type is not None and isinstance(item, dns_error_type):
                return True
            if isinstance(item, socket.gaierror):
                return True
            message = str(item).lower()
            if any(marker in message for marker in dns_markers):
                return True
        return False

    def _fallback_dns_servers(self):
        if self.dns_policy == "system_first":
            return self.dns_servers
        if self.dns_policy == "custom_first":
            return None
        return self._active_dns_servers

    def _format_dns_servers(self, dns_servers):
        if dns_servers is None:
            return "system DNS"
        return ", ".join(dns_servers)

    def _confirm_dns_fallback(self, error, next_dns_servers):
        if not sys.stdin or not sys.stdin.isatty():
            return False

        answer = input(
            "Обнаружена DNS-ошибка при доступе к MOEX: "
            f"{error}\nПопробовать DNS fallback ({self._format_dns_servers(next_dns_servers)})? [y/N]: "
        )
        return answer.strip().lower() in ("y", "yes", "д", "да")

    def _try_dns_fallback(self, error):
        # Resolver переключаем только на ошибках, похожих на DNS.
        # Таймаут чтения или разрыв соединения не означает, что DNS надо менять.
        if self.network_policy == "strict":
            return False
        if self.dns_policy in ("system_only", "custom_only"):
            return False
        if not self._is_dns_error(error):
            return False

        next_dns_servers = self._fallback_dns_servers()
        if next_dns_servers == self._active_dns_servers:
            return False

        if self.network_policy == "interactive" and not self._confirm_dns_fallback(error, next_dns_servers):
            return False

        previous_dns_servers = self._active_dns_servers
        self._active_dns_servers = next_dns_servers
        self._log(
            "info",
            "DNS-ошибка при доступе к MOEX. Переключаю resolver: "
            f"{self._format_dns_servers(previous_dns_servers)} -> {self._format_dns_servers(next_dns_servers)}"
        )
        return True

    @staticmethod
    def _is_aiomoex_empty_response_error(error):
        # Внутри aiomoex пустой ответ иногда проявляется как AttributeError
        # "'NoneType' object has no attribute 'get'". Это сетевой сбой, а не ошибка стратегии.
        return (
            isinstance(error, AttributeError)
            and "'NoneType' object has no attribute 'get'" in str(error)
        )

    def _is_retryable_moex_error(self, error):
        if isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError, ISSMoexError, OSError)):
            return True
        return self._is_aiomoex_empty_response_error(error)

    @staticmethod
    def _metadata_cache_path():
        return os.path.join("files_from_moex", "store_metadata.json")

    def _load_metadata_cache(self):
        # Метаданные грузятся один раз на экземпляр MoexStore.
        # Это снижает число обращений к диску и к MOEX в циклах по контрактам.
        if self._metadata_cache is not None:
            return self._metadata_cache

        if not self.rff:
            self._metadata_cache = {}
            return {}

        cache_path = self._metadata_cache_path()
        if not os.path.isfile(cache_path):
            self._metadata_cache = {}
            return {}

        try:
            with open(cache_path, "r", encoding="utf-8") as file:
                self._metadata_cache = json.load(file)
                return self._metadata_cache
        except (OSError, json.JSONDecodeError):
            self._metadata_cache = {}
            return {}

    def _save_metadata_cache(self, cache):
        if not self.wtf:
            self._metadata_cache = cache
            return

        cache_path = self._metadata_cache_path()
        cache_dir = os.path.dirname(cache_path)
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        self._atomic_write_json(cache_path, cache)
        self._metadata_cache = cache

    @staticmethod
    def _atomic_write_json(file_path, payload):
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        temp_file_path = f"{file_path}.tmp"
        with open(temp_file_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        os.replace(temp_file_path, file_path)

    def _get_cached_sec_details(self, sec_id):
        return self._load_metadata_cache().get("sec_details", {}).get(sec_id)

    def _set_cached_sec_details(self, sec_id, sec_details):
        cache = self._load_metadata_cache()
        cache.setdefault("sec_details", {})[sec_id] = sec_details
        self._save_metadata_cache(cache)

    @staticmethod
    def _history_intervals_cache_key(sec_id, board, market, engine):
        return f"{sec_id}|{board}|{market}|{engine}"

    def _get_cached_history_intervals(self, cache_key):
        return self._load_metadata_cache().get("history_intervals", {}).get(cache_key)

    def _set_cached_history_intervals(self, cache_key, interval_data):
        cache = self._load_metadata_cache()
        cache.setdefault("history_intervals", {})[cache_key] = interval_data
        self._save_metadata_cache(cache)

    @staticmethod
    def _load_cached_dataframe(csv_file_path):
        moex_df = pd.read_csv(csv_file_path, parse_dates=['datetime'])
        required_columns = {'datetime', 'open', 'high', 'low', 'close', 'volume'}
        missing_columns = required_columns - set(moex_df.columns)
        if missing_columns:
            raise ValueError(f"В CSV-кэше отсутствуют столбцы: {', '.join(sorted(missing_columns))}")
        if moex_df.empty:
            raise ValueError("CSV-кэш пустой")

        moex_df.set_index('datetime', inplace=True)
        return moex_df

    @staticmethod
    def _atomic_write_dataframe(df, csv_file):
        directory = os.path.dirname(csv_file)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        temp_csv_file = f"{csv_file}.tmp"
        df.to_csv(temp_csv_file, sep=',', index=True, header=True)
        os.replace(temp_csv_file, csv_file)

    def apply_ssl_patch(self):
        """Compatibility no-op.

        Network settings are now applied per MoexStore instance through
        _create_session(), not by monkey-patching aiohttp globally.
        """
        return

    # def apply_ssl_patch(self):
    #     # Создаем SSL-контекст с отключенной проверкой сертификатов
    #     ssl_context = ssl.create_default_context()
    #     ssl_context.check_hostname = False
    #     ssl_context.verify_mode = ssl.CERT_NONE
    #
    #     # Переопределяем оригинальный метод ClientSession
    #     _original_init = aiohttp.ClientSession.__init__
    #
    #     def _patched_init(self, *args, **kwargs):
    #         if 'connector' not in kwargs:
    #             kwargs['connector'] = aiohttp.TCPConnector(ssl=ssl_context)
    #         _original_init(self, *args, **kwargs)
    #
    #     aiohttp.ClientSession.__init__ = _patched_init

    async def _check_connection(self):
        url = f"https://iss.moex.com/iss/engines.json"
        attempts = 0
        ssl_patched = False

        while attempts < self.max_retries:
            try:
                async with self._create_session() as session:
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
                        timer.sleep(self.retry_delay)
            except aiohttp.ClientError as e:
                print(f"Попытка {attempts + 1}: Не удалось подключиться к MOEX: {e}")
                attempts += 1
                if attempts < self.max_retries:
                    timer.sleep(self.retry_delay)
            except Exception as e:
                raise ConnectionError(f"Не удалось подключиться к MOEX: {e}")

    def _ensure_connection_checked(self):
        if self._connection_checked:
            return

        try:
            asyncio.run(self._check_connection())
        except Exception as error:
            self._log(
                "info",
                f"Не удалось выполнить предварительную проверку MOEX: {error}. Продолжаю загрузку напрямую..."
            )
        self._connection_checked = True

    def get_data(self, sec_id, fromdate, todate, tf='1h', name=None):
        fd = self.validate_fromdate(fromdate)
        td = self.validate_todate(todate)

        self._validate_basic_inputs(sec_id, fd, td, tf, name)
        csv_file_path = f"files_from_moex/{sec_id}_{tf}_{fd.strftime('%d%m%Y')}_{td.strftime('%d%m%Y')}.csv"
        moex_df = None
        if self.rff and os.path.isfile(csv_file_path):
            try:
                moex_df = self._load_cached_dataframe(csv_file_path)
                print(f'Для {sec_id} с указанными параметрами котировки найдены на Диске. Загружаю...')
            except (OSError, ValueError, KeyError, pd.errors.ParserError) as error:
                print(f'CSV-кэш для {sec_id} поврежден или неполный: {error}. Загружаю с Биржи...')

        if moex_df is None:
            # Проверка значений
            self._validate_inputs(sec_id, fd, td, tf, name)
            self._ensure_connection_checked()
            moex_data = self._get_candles_history_with_retries(sec_id, fd, td, tf)
            if not moex_data:
                raise ValueError(
                    f"Биржа не вернула котировки для {sec_id}: "
                    f"тайм-фрейм {tf}, период с {fd} по {td}."
                )
            # Готовим итоговый дата-фрейм для backtrader.cerebro и записываем на диск в папку files_from_moex
            moex_df = self._make_df(moex_data, self.sec_details[sec_id]['market'], csv_file_path)  # формируем файл с историей
        data = bt.feeds.PandasData(dataname=moex_df, fromdate=fd, todate=td, name=name)
        return data

    @staticmethod
    def validate_fromdate(inp_date):
        if isinstance(inp_date, datetime):
            return inp_date
        elif isinstance(inp_date, str):
            for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
                try:
                    return datetime.strptime(inp_date, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Неверный формат даты: {inp_date}. Используйте тип datetime или тип "
                             f"str в формате 'YYYY-MM-DD' или 'DD-MM-YYYY'.")
        else:
            raise ValueError(f"Дата должна быть типа datetime или str, получили тип: {type(inp_date).__name__}, "
                             f"значение: {inp_date}")

    @staticmethod
    def validate_todate(inp_date):
        if isinstance(inp_date, datetime):
            if inp_date.time() == time(0, 0, 0):
                return datetime.combine(inp_date.date(), time(23, 59, 59, 999990))
            return inp_date
        elif isinstance(inp_date, str):
            for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
                try:
                    _date = datetime.strptime(inp_date, fmt)
                    return datetime.combine(_date.date(), time(23, 59, 59, 999990))
                except ValueError:
                    continue
            raise ValueError(f"Неверный формат даты: {inp_date}. Используйте тип datetime или тип "
                             f"str в формате 'YYYY-MM-DD' или 'DD-MM-YYYY'.")
        else:
            raise ValueError(f"Дата должна быть типа datetime или str, получили тип: {type(inp_date).__name__}, "
                             f"значение: {inp_date}")

    def _validate_basic_inputs(self, sec_id, fromdate, todate, tf, name):
        if not isinstance(sec_id, str):
            raise ValueError(f"Тикер sec_id должен быть str, получен {type(sec_id).__name__}")
        if name is not None and not isinstance(name, str):
            raise ValueError(f"Тип имени источника данных должен быть str, получен {type(name).__name__}")
        if fromdate >= todate:
            raise ValueError(f"fromdate ({fromdate}) должен быть меньше (раньше) todate ({todate}), \n"
                             f"для получения котировок за один день используйте, например для 2023-06-20: \n"
                             f"fromdate = '2023-06-20', todate = '2023-06-21'")

        if tf not in TF:
            raise ValueError(
                f"Тайм-фрейм для {sec_id} должен быть одним из списка: {list(TF.keys())}, получен: {tf = }")

    def _validate_inputs(self, sec_id, fromdate, todate, tf, name):
        self._validate_basic_inputs(sec_id, fromdate, todate, tf, name)

        sec_details_available_locally = sec_id in self.sec_details
        if sec_id not in self.sec_details:
            cached_sec_details = self._get_cached_sec_details(sec_id)
            if cached_sec_details:
                self.sec_details[sec_id] = cached_sec_details
                sec_details_available_locally = True
            else:
                sec_info = self._get_instrument_info_with_retries(sec_id)

                if sec_info[-1] is None:
                    raise ValueError(f"Инструмент с sec_id {sec_id} не найден на Бирже")

                # print(f'Инструмент {sec_id} найден на Бирже')
                self.sec_details[sec_id] = dict(
                    sectype=sec_info[0],
                    grouptype=sec_info[1],
                    assetcode=sec_info[2],
                    board=sec_info[3],
                    market=sec_info[4],
                    engine=sec_info[5]
                )
                self._set_cached_sec_details(sec_id, self.sec_details[sec_id])

        # Проверка доступных интервалов котировок get_history_intervals
        intervals_cache_key = self._history_intervals_cache_key(
            sec_id,
            self.sec_details[sec_id]['board'],
            self.sec_details[sec_id]['market'],
            self.sec_details[sec_id]['engine'],
        )
        if intervals_cache_key in self._history_intervals_cache:
            interval_data = self._history_intervals_cache[intervals_cache_key]
        else:
            interval_data = self._get_cached_history_intervals(intervals_cache_key)
            if interval_data is None:
                try:
                    interval_data = self._get_history_intervals_with_retries(
                        sec_id,
                        self.sec_details[sec_id]['board'],
                        self.sec_details[sec_id]['market'],
                        self.sec_details[sec_id]['engine'],
                    )
                except ConnectionError as error:
                    if self.rff and sec_details_available_locally:
                        # Если описание инструмента уже есть локально, то недоступность
                        # справочного endpoint с интервалами не должна блокировать загрузку свечей.
                        self._log(
                            "info",
                            f"Не удалось проверить интервалы котировок {sec_id} на MOEX: {error}. "
                            "Использую локальные сведения об инструменте и продолжаю загрузку..."
                        )
                        return
                    raise
                if interval_data is not None:
                    self._set_cached_history_intervals(intervals_cache_key, interval_data)
            self._history_intervals_cache[intervals_cache_key] = interval_data

        # [{'begin': '2022-05-18 11:45:00', 'end': '2024-03-21 13:59:00', 'interval': 1,
        # 'board_group_id': 45} {'begin': '2022-04-01 00:00:00', 'end': '2024-01-01 00:00:00', 'interval': 4,
        # 'board_group_id': 45}, ... ]

        if interval_data is None:
            raise ValueError(f"На Бирже нет доступных интервалов котировок для инструмента {sec_id}.")

        # Если запрошен тайм-фрейм 5, 15 или 30 мин, то проверяем наличие на Биржи котировок
        # с тайм-фреймом 1 мин, так как из них будут приготовлены котировки для 5, 15 или 30 мин.
        user_tf = 1 if TF[tf] in (5, 15, 30) else TF[tf]
        valid_interval = next((item for item in interval_data if item['interval'] == user_tf), None)

        if not valid_interval:
            raise ValueError(f"Тайм-фрейм {tf} не доступен для инструмента {sec_id}")

        valid_begin = datetime.strptime(valid_interval['begin'], '%Y-%m-%d %H:%M:%S')
        valid_end = datetime.strptime(valid_interval['end'], '%Y-%m-%d %H:%M:%S')

        if fromdate > valid_end:
            raise ValueError(f"fromdate ({fromdate}) для {sec_id} и тайм-фрейма '{tf}' должен быть меньше (раньше) {valid_end}, \n"
                             f"валидный интервал с {valid_interval['begin']} по {valid_interval['end']}")

        if todate < valid_begin:
            raise ValueError(f"todate ({todate}) для {sec_id} и тайм-фрейма '{tf}' должен быть больше (позже) {valid_begin}, \n"
                             f"валидный интервал с {valid_interval['begin']} по {valid_interval['end']}")

    def _get_candles_history_with_retries(self, sec_id, fromdate, todate, tf):
        # Основная загрузка свечей: retry, DNS fallback и понятная финальная ошибка.
        attempts = max(1, self.max_retries)
        last_error = None

        for attempt in range(1, attempts + 1):
            try:
                return asyncio.run(self._get_candles_history(sec_id, fromdate, todate, tf))
            except Exception as error:
                if not self._is_retryable_moex_error(error):
                    raise
                last_error = error
                if self._try_dns_fallback(error):
                    continue
                if attempt == attempts:
                    break
                self._log(
                    "info",
                    f"Попытка {attempt}: не удалось загрузить котировки {sec_id}. "
                    f"Повтор через {self.retry_delay} сек. Ошибка: {error}"
                )
                timer.sleep(self.retry_delay)

        raise ConnectionError(
            f"Не удалось загрузить котировки {sec_id} за {attempts} попытки: {last_error}"
        ) from last_error

    def _get_instrument_info_with_retries(self, sec_id):
        # Справочник инструмента нужен до загрузки свечей: рынок, board и engine.
        attempts = max(1, self.max_retries)
        last_error = None

        for attempt in range(1, attempts + 1):
            try:
                return asyncio.run(self.get_instrument_info(sec_id))
            except Exception as error:
                if not self._is_retryable_moex_error(error):
                    raise
                last_error = error
                if self._try_dns_fallback(error):
                    continue
                if attempt == attempts:
                    break
                timer.sleep(self.retry_delay)

        raise ConnectionError(
            f"Не удалось получить описание инструмента {sec_id} за {attempts} попытки: {last_error}"
        ) from last_error

    def _get_history_intervals_with_retries(self, sec_id, board, market, engine):
        # Интервалы истории используются для ранней проверки периода и тайм-фрейма.
        attempts = max(1, self.max_retries)
        last_error = None

        for attempt in range(1, attempts + 1):
            try:
                return asyncio.run(self.get_history_intervals(sec_id, board, market, engine))
            except Exception as error:
                if not (self._is_retryable_moex_error(error) or isinstance(error, ConnectionError)):
                    raise
                last_error = error
                if self._try_dns_fallback(error):
                    continue
                if attempt == attempts:
                    break
                timer.sleep(self.retry_delay)

        raise ConnectionError(
            f"Не удалось получить интервалы котировок {sec_id} за {attempts} попытки: {last_error}"
        ) from last_error

    async def get_instrument_info(self, secid):
        async with self._create_session() as session:
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

    async def get_history_intervals(self, sec_id, board, market, engine):
        async with self._create_session() as session:
            market_error = None
            try:
                data = await aiomoex.get_market_candle_borders(session, security=sec_id, market=market, engine=engine)
                if data:
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError, ISSMoexError, OSError) as error:
                market_error = error

            try:
                data = await aiomoex.get_board_candle_borders(session, security=sec_id, board=board, market=market,
                                                              engine=engine)
                if data:
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError, ISSMoexError, OSError) as error:
                if market_error:
                    raise ConnectionError(
                        f"Не удалось получить интервалы котировок для {sec_id}: "
                        f"market error={market_error}; board error={error}"
                    ) from error
                raise

            if market_error:
                raise ConnectionError(
                    f"Не удалось получить интервалы котировок для {sec_id}: market error={market_error}"
                ) from market_error

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

        async with self._create_session() as session:
            start_time = timer.time()
            if tf in (1, 10, 60, 24):
                estimated_time = self.get_estimated_time(delta, tf)
                print(f'Ожидаемое время загрузки данных для {sec_id} (зависит от загрузки серверов MOEX): {estimated_time:.0f} сек.')
                timer.sleep(0.05)
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

            end_time = timer.time()
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

    def _make_df(self, data, market, csv_file):
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
            try:
                self._atomic_write_dataframe(df, csv_file)
                print(f'Котировки записаны в файл "{csv_file}"')
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

    getdata = get_data

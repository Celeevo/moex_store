# moex_store: пользовательская инструкция

Эта инструкция описывает текущую оптимизированную версию `moex_store`: как загружать котировки MOEX, как работает локальный кэш, что означают сетевые настройки, как проверять фьючерсные даты и как запускать диагностику.

## Быстрый старт

Минимальный пример:

```python
from moex_store import MoexStore

store = MoexStore()

data = store.getdata(
    sec_id="AFLT",
    fromdate="2023-01-01",
    todate="2023-01-10",
    tf="1h",
    name="AFLT",
)
```

`getdata()` возвращает `backtrader.feeds.PandasData`, поэтому результат можно сразу передавать в `cerebro.adddata(data)`.

## Основные параметры MoexStore

```python
store = MoexStore(
    write_to_file=True,
    read_from_file=True,
    max_retries=3,
    retry_delay=2,
    dns_policy="system_first",
    network_policy="auto",
    log_level="info",
)
```

- `write_to_file=True` - сохранять загруженные котировки и метаданные в папку `files_from_moex`.
- `read_from_file=True` - сначала искать котировки и метаданные на диске, и только потом идти на MOEX.
- `max_retries=3` - число повторных попыток при временных сетевых ошибках.
- `retry_delay=2` - пауза между retry в секундах.
- `dns_policy="system_first"` - сначала системный DNS Windows, при DNS-сбое fallback на список публичных DNS.
- `network_policy="auto"` - автоматически включать DNS fallback только при DNS-ошибках.
- `log_level="info"` - печатать диагностику retry и DNS fallback.

## Кэш котировок

Котировки сохраняются в `files_from_moex` в CSV-файлы вида:

```text
EuH3_15m_15122022_16032023.csv
```

В имени файла:

- `EuH3` - код инструмента;
- `15m` - тайм-фрейм;
- `15122022` - дата начала котировок в файле;
- `16032023` - дата конца котировок в файле.

Если такой файл уже есть и `read_from_file=True`, библиотека читает его с диска и не делает сетевые запросы к MOEX для этих котировок.

Если CSV поврежден, пустой или в нем не хватает обязательных колонок, библиотека не использует его молча, а пишет диагностическое сообщение и пробует загрузить данные с MOEX заново.

## Кэш метаданных

Помимо CSV с котировками библиотека сохраняет справочные данные:

```text
files_from_moex/store_metadata.json
files_from_moex/futures_metadata.json
```

`store_metadata.json` хранит:

- описание инструмента: рынок, режим торгов, engine, тип бумаги;
- доступные интервалы истории котировок.

`futures_metadata.json` хранит:

- историю фьючерсных контрактов по базовому активу;
- сведения о фьючерсных инструментах.

Если MOEX временно недоступна, но нужные метаданные уже есть в этих файлах, библиотека старается использовать локальные данные и не падать на справочных запросах.

## Сетевые сбои и DNS fallback

Основная проблема, которую закрывает текущая версия: MOEX может быть доступна нестабильно, особенно без VPN, а `aiomoex` иногда падает на частично пустом ответе.

Библиотека теперь обрабатывает:

- DNS timeout;
- `aiohttp.ClientError`;
- `asyncio.TimeoutError`;
- временные обрывы соединения;
- пустой ответ MOEX;
- ошибку `AttributeError: 'NoneType' object has no attribute 'get'` внутри `aiomoex`.

По умолчанию используется режим:

```python
store = MoexStore(
    network_policy="auto",
    dns_policy="system_first",
)
```

Это значит:

1. Сначала используется системный DNS.
2. Если ошибка похожа именно на DNS-проблему, библиотека переключается на альтернативный resolver.
3. Если ошибка не DNS, resolver не меняется, а выполняется обычный retry.

Доступные `dns_policy`:

- `system_first` - начать с системного DNS, при DNS-сбое перейти на публичные DNS.
- `custom_first` - начать с публичных/custom DNS, при DNS-сбое вернуться на системный DNS.
- `system_only` - использовать только системный DNS.
- `custom_only` - использовать только заданные DNS.

Задать свои DNS можно так:

```python
store = MoexStore(
    dns_servers=("1.1.1.1", "8.8.8.8"),
    dns_policy="system_first",
)
```

Или через переменную окружения:

```powershell
$env:MOEX_STORE_DNS_SERVERS="1.1.1.1,8.8.8.8"
```

## Диагностика вывода

Параметр `log_level` управляет диагностическими сообщениями:

- `silent` - минимум сообщений;
- `error` - только ошибки;
- `info` - DNS fallback, retry, обход недоступной предварительной проверки;
- `debug` - дополнительно показывает параметры создаваемой сетевой сессии.

Для проверки сетевого поведения удобно запускать так:

```python
store = MoexStore(log_level="debug")
```

## Фьючерсы: expdate и prevexpdate

Функции:

```python
store.futures.expdate(sec_id)
store.futures.prevexpdate(sec_id)
```

`expdate(sec_id)` возвращает дату экспирации самого контракта `sec_id`.

`prevexpdate(sec_id)` возвращает дату экспирации предыдущего контракта относительно `sec_id`.

Важно: даты в имени CSV-файла и даты `expdate` / `prevexpdate` - это разные вещи.

- В имени CSV указаны даты начала и конца котировок, лежащих в файле.
- `expdate` - дата экспирации контракта.
- `prevexpdate` - дата экспирации предыдущего контракта.

Пример загрузки цепочки фьючерсов:

```python
from moex_store import MoexStore

store = MoexStore()
contracts = ["EuH3", "EuM3", "EuU3"]

for sec_id in contracts:
    fromdate = store.futures.prevexpdate(sec_id)
    todate = store.futures.expdate(sec_id)

    data = store.getdata(
        sec_id=sec_id,
        fromdate=fromdate,
        todate=todate,
        tf="15m",
        name=sec_id,
    )
```

## Дубли и старые фьючерсные коды

В истории MOEX встречаются дубли одного контракта:

```text
EuH8
EuH8_2018
```

Также встречаются технические записи и календарные спреды:

```text
EuZ6EuH7
EuU6EuZ6
```

Библиотека хранит raw-историю, но для поиска предыдущего контракта строит нормализованную цепочку:

- календарные спреды не выбираются как основной контракт;
- технические дубли с `_YYYY` учитываются, но при равных условиях обычный код предпочтительнее;
- порядок от MOEX не считается надежным, цепочка сортируется по дате экспирации.

## Диагностические команды

Посмотреть сырую и нормализованную историю фьючерсов:

```powershell
.\.venv\Scripts\python.exe .\tools\diagnose_futures.py Eu --contracts EuH6 EuM2 EuU8 EuZ5 EuH9 EuH0_2010 EuH0_2000
```

Результат будет записан в:

```text
files_from_moex/futures_diagnostics_Eu.json
```

Проверить старые контракты до заданного года:

```powershell
.\.venv\Scripts\python.exe .\tools\diagnose_futures_before_year.py Eu --before-year 2020 --limit 30
```

Обновить локальный `futures_metadata.json` по данным диагностики:

```powershell
.\.venv\Scripts\python.exe .\tools\diagnose_futures.py Eu --update-cache
```

## Тестирование

Основной набор unit-тестов:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

Проверка, что все файлы компилируются:

```powershell
.\.venv\Scripts\python.exe -m compileall moex_store tests tools test.py
```

Реальный тестовый скрипт:

```powershell
.\.venv\Scripts\python.exe .\test.py
```

Если хотите проверить поведение без кэша, перед запуском можно временно переименовать или удалить папку `files_from_moex`. Делать это стоит только если понятно, что все данные можно заново скачать.

## Что пока отложено

В текущий пакет намеренно не включены:

- загрузка свечей сегментами по датам;
- полноценная работа через SOCKS/VPN/proxy как основной сценарий.

Эти темы лучше дорабатывать отдельно после проверки текущей устойчивой версии на нескольких ПК.

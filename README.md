# Хранилище (Store) Источников данных Московской биржи MOEX для Backtrader.

Содержание:
1. [Назначение](#назначение)
2. [Установка](#установка)
3. [Применение в Backtrader](#применение-в-backtrader)
4. [Информация по фьючерсам MOEX](#информация-по-фьючерсам-moex)
   4.1. [Пример использования в backtrader](#пример-использования-в-_backtrader_)
   4.2. [Справка по новым функциям](#справка-по-новым-функциям)


## Назначение

Позволяет осуществить загрузку исторических котировок по инструментам Московской Бирже MOEX с 
информационного сервера биржи iss.moex.com прямо из кода тестирования вашей стратегии в [_backtrader_](https://www.backtrader.ru/). Не требует 
предварительной регистрации и аутентификации при запросе данных. Больше не нужно искать данные по историческим 
котировкам в открытых источниках, сохранять их в файлы и регулярно обновлять. 

## Установка

   ```pip install moex-store```

Установит библиотеку и все необходимые зависимости. Требуется Python `3.9` и выше.

## Применение в Backtrader

### Получение источников данных (котировок)

1. Импортируйте класс Хранилища `MoexStore` из библиотеки `moex_store` в скрипте, где вы инициализируете `cerebro`.
   
   ```python
   from moex_store import MoexStore
   
   ...
   ```
   
2. Создайте экземпляр Хранилища, сохраните его в переменную.

   ```python
   store = MoexStore()
   ```
   
   Хранилище имеет один устанавливаемый пользователем атрибут `write_to_file` (по умолчанию True), управляющий записью 
   полученных с Биржи котировок на диск в файл `csv` для их визуальной проверки. Запись осуществляется в подпапку 
   `files_from_moex`, создаваемую в папке, где лежит ваш скрипт. Если запись файлов не требуется, установите 
   этот атрибут в False при создании Хранилища:  

   ```python
   store = MoexStore(write_to_file=False)
   ```
   
3. Получение котировок осуществляется вызовом метода `get_data` (или `getdata`) экземпляра Хранилища `store`. На примере 
   акций Аэрофлота (тикер на бирже `AFLT`), сохраняем исторические котировки с тайм-фреймом 1 минута с 01 января 2023 
   по 01 января 2024 года в источник данных (DataFeed) `data`, присваивая ему имя `aflt`:

   ```python
   data = store.getdata(sec_id='AFLT', fromdate='01-01-2023', todate='01-01-2024', tf='1m', name='Аэрофлот')
   ```
   
   Все аргументы метода `get_data` являются обязательными, кроме `name` (по умолчанию `None`):

   - `sec_id` - тикер инструмента Мосбиржи ([Код инструмента в торговой системе](https://www.moex.com/ru/spot/issues.aspx)).

   - `fromdate` - дата, с которой будут загружаться котировки.

   - `todate` - дата, по которую будут загружаться котировки.

       Допустимые форматы для `fromdate` и `todate`:
       - datetime (`datetime.datetime(2023, 1, 1)`).
       - строка в формате `'YYYY-MM-DD'` или `'DD-MM-YYYY'`, как в примере выше.

   - `tf` - тайм-фрейм котировки. Допустимые значения:

      - `1m`: 1 минута, 
      - `5m`: 5 минут, 
      - `10m`: 10 минут, 
      - `15m`: 15 минут, 
      - `30m`: 30 минут, 
      - `1h`: 60 минут, 
      - `1d`: день, 
      - `1w`: неделя, 
      - `1M`: месяц, 
      - `1q`: квартал

   - `name` - имя возвращаемого источника данных для отображения на графиках платформы _backtrader_.

   Метод `get_data` возвращает объект [feeds.PandasData](https://www.backtrader.ru/docu/datafeed/datafeed_pandas/) 
   экосистемы _backtrader_, поэтому его можно сразу подгружать в `cerebro` с помощью `cerebro.adddata()`.


4. Добавление Источника данных в движок [cerebro](https://www.backtrader.ru/docu/cerebro/cerebro/) осуществляется стандартно:

   ```python
   cerebro.adddata(data)
   ```

Полный код примера:

```python
from __future__ import (absolute_import, division, print_function,
                    unicode_literals)
import backtrader as bt
from moex_store import MoexStore

def runstrat():
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore()
    data = store.getdata("AFLT", "01-01-2023", "01-01-2024", "1h", 'aflt-2023-hour')

    cerebro.adddata(data)
    cerebro.run()
    cerebro.plot(style="bar")


if __name__ == '__main__':
    runstrat()
```

Вывод покажет загруженный Источник данных:

![pict1.png](assets%2Fpict1.png)

Экземпляр Хранилища `store` позволяет осуществлять загрузку нескольких источников данных:

```python
from __future__ import (absolute_import, division, print_function,
                    unicode_literals)
import backtrader as bt
from moex_store import MoexStore
from datetime import datetime

def runstrat():
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=False)
    tf = '1d'
    fromdate = '01-01-2023'
    todate = datetime.today()
    for tiker in ('GAZP', 'NLMK', 'SIH4'):
        data = store.get_data(sec_id=tiker, 
                              fromdate=fromdate, 
                              todate=todate, 
                              tf=tf, name=tiker)
        cerebro.adddata(data)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()
```

Источники данных, добавленные в `cerebro`:

![pict2.png](assets%2Fpict2.png)

## Информация по фьючерсам MOEX

Начиная с версии `0.0.7`, библиотека дает возможность получить имена, даты экспирации и другую 
полезную информацию о фьючерсных контрактах. Функции для получения информации помещены в модуль библиотеки `futures`.

Для получения информации о фьючерсе нужно знать код его базового актива. Что такое код базового актива 
фьючерсного контракта? Это уникальный идентификатор (тикер или символ), который используется для обозначения базового 
актива, и на который ссылается фьючерс.

ℹ️ **Информация:**
    Примеры кодов базовых активов:

    1. Фьючерсы на индексы:
        * Фьючерсы на индекс ММВБ (MOEX): `MIX`
        * Фьючерсы на Индекс РТС: `RTS`
    2. Валютные фьючерсы:
        * Фьючерсы на курс доллар США - российский рубль: `Si`
        * Фьючерсы на курс китайский юань – российский рубль: `CNY`
    3. Товарные фьючерсы:
        * Фьючерсы на нефть марки Brent: `BR`
        * Фьючерсы на золото: `GOLD`
    4. Фьючерсы на акции:
        * Фьючерсы на обыкновенные акции ПАО Сбербанк: `SBRF`
        * Фьючерсы на обыкновенные акции ПАО Газпром: `GAZR`

Чтобы найти код базового актива можно воспользоваться функцией:

```python
store.futures.get_all_active_futures(show_table=True)
```

Функция запросит биржу, вернет и откроет в браузере таблицу со всеми активными торгуемыми фьючерсами _на момент ее вызова_. 
Html файл с этой таблицей будет сохранен в папке вашего скрипта:

[active_futures.html](assets%2Factive_futures.html)

⚠️ **Внимание:**
    Активный фьючерсный контракт — это фьючерсный контракт с ближайшей к текущей датой экспирации, который 
    характеризуется наибольшей ликвидностью и объемом торгов на рынке.

В колонках таблицы: 

* код текущего активного фьючерсного контракта `SECID`, 
* его короткое имя `SHORTNAME`, 
* искомый код базового актива `ASSETCODE`, 
* имя фьючерсного контракта `CONTRACTNAME`, 
* тип группы фьючерсного контракта `GROUPTYPE` (пригодится для определения комиссий, см. пост «Комиссия при работе с фьючерсами MOEX»), 
* дата начало торгов текущего активного фьючерсного контракта `FRSTTRADE`, 
* дата его экспирации `LASTTRADEDATE`, 
* количество знаков после запятой в значениях котировок `DECIMALS`,
* минимальный шаг цены `MINSTEP`,
* стоимость шага цены `STEPPRICE`,
* количество контрактов в лоте `LOTVOLUME`,
* и начальное значение гарантийного обеспечения `INITIALMARGIN`

### Пример использования в _backtrader_

В качестве примера использования новых функций `moex-story` разберем кейс, в котором получим котировки фьючерсных 
контрактов на индекс РТС за один прошедший год, начиная с текущей даты:

```python linenums="1"
1. from __future__ import (absolute_import, division, print_function,
2.                         unicode_literals)
3. import backtrader as bt
4. from datetime import datetime, timedelta
5. from moex_store import MoexStore
6. 
7.
8. def runstrat():
9.     cerebro = bt.Cerebro(stdstats=False)
10.    store = MoexStore()
11.    from_date = datetime.today() - timedelta(days=365)  # 2023-09-22
12.    to_date = datetime.today()                          # 2024-09-21
13.    contracts = store.futures.get_contracts_between(asset='RTS', 
14.                                                    from_date=from_date, 
15.                                                    to_date=to_date)
16.    # contracts - ['RIZ3', 'RIH4', 'RIM4', 'RIU4', 'RIZ4']
17.    
18.    for tiker in contracts:
19.        from_date = store.futures.get_previous_contract_exp_date(tiker)
20.        to_date = store.futures.get_contract_exp_date(tiker)
21.        data = store.getdata(sec_id=tiker, 
22.                             fromdate=from_date, todate=to_date, 
23.                             tf='1h', name=tiker)
24.        cerebro.adddata(data)
25.
26.    cerebro.run()
27.    cerebro.plot(style='bar')
28.
29.
30. if __name__ == '__main__':
31.     runstrat()
```

Разберем код по строкам:

* 9 - инициализируем движок `cerebro`
* 10 - инициализируем экземпляр хранилища источников данных `moex-story`
* 11-12 - задаем даты с/по, за которые попросим `moex-story `вернуть коды активных фьючерсных контрактов
* 13 - вызов функции `get_contracts_between()` для получения списка кодов фьючерсных контрактов с даты `from_date` по `to_date`.

    Функция принимает на вход аргументы:

    * `asset` - код базового актива искомого фьючерса, у нас это `RTS`, 
    * `from_date` - дата, на которую функция вернет начальный активный фьючерсный контракт, по умолчанию - текущая дата минус один год,
    * `to_date` - дата, на которую функция вернет конечный активный фьючерсный контракт, по умолчанию - текущая дата.
    
      Тип `from_date` и `to_date` может быть `datetime` или строка вида `'YYYY-MM-DD'` или '`DD-MM-YYYY'`

    Функция вернет список с кодами фьючерсных контрактов, от раннего к позднему. Список начнется с кода активного 
    фьючерсного контракта на дату `from_date` и закончится кодом активного 
    фьючерсного контракта на дату `to_date`. В нашем случае: `['RIZ3', 'RIH4', 'RIM4', 'RIU4', 'RIZ4']`
  
* 18 - проходим циклом по полученному списку с фьючерсными контрактами и для каждого:
* 19 - получаем дату экспирации предыдущего контракта функцией `get_previous_contract_exp_date(tiker)`,
* 20 - дату экспирации самого контракта функцией `get_contract_exp_date(tiker)`,
* 21 - получаем котировки каждого контракта с помощью функции `store.getdata()`, начиная с даты экспирации предыдущего 
  контракта по дату экспирации текущего в цикле. Получаем котировки в виде экземпляра встроенного в _backtrader_ класса 
  источника данных `PandasData`. Подробное описание функции см. в посте [MOEX Store](https://www.backtrader.ru/blog/2024/08/01/moex-store/).
* 24 - добавляем полученный источник данных с котировками фьючерсного контракта в движок `cerebro`.

Котировки фьючерсных контрактов загружены в `cerebro` и готовы к склейке или другим манипуляциям!

![print.png](assets%2Fprint.png)

Обратите внимание, строго говоря, мы загрузи котировки не за год с текущей даты. На дату год назад мы определили 
действующий тогда активный фьючерсный контракт (под капотом функции `get_contracts_between()`) и загрузили его котировки 
с момента экспирации предшествующего ему фьючерсного контракта. Обрезать котировки ровно на год назад, конечно, труда не составит.

### Справка по новым функциям

#### **get_all_active_futures(show_table)**
* Алиас: `all_active`
* Принимает на вход параметр `show_table` (по умолчанию `True`):
    * `True` - результат выводится в виде таблицы в файл `html`, который будет открыт в браузере по умолчанию и сохранен в папку скрипта. Возвращает `None`.
    * `False` - результат вернет в виде списка списков.
* Возвращает информацию об активных фьючерсных контрактах биржи MOEX. Информация представлена в виде таблицы с колонками:
    * код текущего активного фьючерсного контракта `SECID`, 
    * его короткое имя `SHORTNAME`, 
    * код базового актива фьючерсного контракта `ASSETCODE`, 
    * имя фьючерсного контракта `CONTRACTNAME`, 
    * тип группы фьючерсного контракта `GROUPTYPE`, 
    * дата начало торгов текущего активного фьючерсного контракта `FRSTTRADE`, 
    * дата его экспирации `LASTTRADEDATE`, 
    * количество знаков после запятой в значениях котировок `DECIMALS`,
    * минимальный шаг цены `MINSTEP`,
    * стоимость шага цены `STEPPRICE`,
    * количество контрактов в лоте `LOTVOLUME`,
    * начальное значение гарантийного обеспечения `INITIALMARGIN`

#### **get_sec_info(sec_id)**

* Алиас: `info`
* Принимает код инструмента `sec_id` (фьючерсного контракта). Ответ вернется, только если тип инструмента - фьючерс.
* Возвращает словарь с информацией об инструменте. Ключи словаря:
      * Тип ценной бумаги `sectype`
      * Тип группы инструмента `grouptype`
      * Код базового актива фьючерсного контракта `assetcode` 
      * Режим торгов инструмента `board`
      * Тип финансового рынка инструмента `market`
      * Торговая система инструмента `engine`

#### **get_asset_code(sec_id):**

* Алиас: `asset`
* Принимает код инструмента `sec_id` (фьючерсного контракта)
* Возвращает код базового актива фьючерса `assetcode` (строка) 


#### **get_history_stat(asset, to_active, show_table)**
* Алиас: `stat`
* Принимает на вход: 
    * код базового актива фьючерса `asset`
    * параметр `to_active` (по умолчанию `True`):
        * `True` - вернет данные, ограничившись текущим активным фьючерсным контрактом.
        * `False` - вернет полные данные.
    * параметр `show_table` (по умолчанию `True`):
        * `True` - результат выводится в виде таблицы в файле `html`, который будет открыт в браузере по умолчанию и сохранен в папку скрипта. Возвращает `None`.
        * `False` - результат вернет в виде списка списков.
* Возвращает таблицу (или список списков) интервалов торгов всех фьючерсных контрактов для указанного базового актива (список списков). Таблица содержит следующие колонки:
    * Код контракта `secid`
    * Краткое имя контракта `shortname`
    * Дата начала торгов `startdate`
    * Дата экспирация `expdate`
    * Код базового актива `assetcode`
    * Базовый актив `underlyingasset`
    * Торгуется контракт или уже нет `is_traded` (0 - нет, 1- торгуется)
* Пример - результат вызова `get_history_stat('NG')`: [exp_dates.html](assets%2Fexp_dates.html)

#### **get_history_list(asset, to_active)**
* Алиас: `list`
* Принимает код базового актива фьючерса `asset` и параметр `to_active` (по умолчанию `True`):
    * `True` - вернет данные, ограничившись текущим активным фьючерсным контрактом.
    * `False` - вернет полные данные.
* Возвращает список кодов всех торговавшихся фьючерсных контрактов, от поздних к текущему активному (список строк).
* Пример - результат вызова `get_history_list('CNY')`: ['CRM2', 'CRU2', 'CRZ2', 'CRH3', 'CRM3', 'CRU3', 'CRZ3', 'CRH4', 'CRM4', 'CRU4', 'CRZ4']

#### **get_n_last_contracts(asset, n, to_active)**
* Алиас: `nlast`
* Принимает: 
    * код базового актива фьючерса `asset`, 
    * `n` - количество крайних контрактов в списке всех когда-либо торгуемых (`0` - все), 
    * `to_active` - см. выше.
* Возвращает список кодов `n` последних торговавшихся фьючерсных контрактов, от раннего к позднему.

#### **get_contracts_between(asset, from_date, to_date)**
* Алиас: `contracts_between`
* принимает на вход аргументы:
    * `asset` - код базового актива фьючерса, 
    * `from_date` - дата, на которую функция вернет начальный активный фьючерсный контракт, по умолчанию - текущая дата минус один год,
    * `to_date` - дата, на которую функция вернет конечный активный фьючерсный контракт, по умолчанию - текущая дата.
    
      Тип `from_date` и `to_date` может быть `datetime` или строка вида `'YYYY-MM-DD'` или '`DD-MM-YYYY'`

* возвращает список с кодами фьючерсных контрактов, от раннего к позднему, от кода активного 
фьючерсного контракта на дату `from_date` до кода активного фьючерсного контракта на дату `to_date`. 

#### **get_active_contract(asset, date)**
* Алиас: `active` 
* Принимает код базового актива фьючерса `asset` и `date` (по умолчанию - текущая дата) - дату, на которую нужно вернуть код активного фьючерсного контракта.
* Возвращает код активного фючерсного контракта, действовавшего на дату `date`.

#### **get_contract_exp_date(sec_id)**
* Алиас: `expdate`
* Принимает код инструмента `sec_id` (фьючерсного контракта)
* Возвращает дату экспирации фьючерсного контракта `sec_id`. Тип - строка формата `'YYYY-MM-DD'`

#### **get_previous_contract_exp_date(sec_id)**
* Алиас: `prevexpdate`
* Принимает код инструмента `sec_id` (фьючерсного контракта)
* Возвращает дату экспирации фьючерсного контракта, предшествующего контракту `sec_id`. Тип - строка формата `'YYYY-MM-DD'`








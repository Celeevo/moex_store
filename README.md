# Хранилище (Store) Источников данных Московской биржи MOEX для Backtrader.

Содержание:
1. [Назначение](#назначение)
2. [Установка](#установка)
3. [Применение в Backtrader](#применение-в-backtrader)


## Назначение

Позволяет осуществить загрузку исторических котировок по инструментам Московской Бирже MOEX с 
информационного сервера биржи iss.moex.com прямо из кода тестирования вашей стратегии в [_backtrader_](https://www.backtrader.ru/). Не требует предварительной регистрации и аутентификации при запросе данных.
Больше не нужно искать данные по историческим котировкам в открытых источниках, сохранять их в файлы и регулярно 
обновлять. 

## Установка

   ```pip install moex-store```

Установит библиотеку и все необходимые зависимости. Требуется Python `3.10` и выше.

## Применение в Backtrader

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
   `files_from_moex`, создаваемую в папке, где лежит ваш скрипт стратегии. Если запись файлов не требуется, установите 
   этот атрибут в False при создании Хранилища:  

   ```python
   store = MoexStore(write_to_file=False)
   ```
   
3. Получение котировок осуществляется вызовом метода `get_data` экземпляра Хранилища `store`. На примере акций Аэрофлота 
   (тикер на бирже `AFLT`), сохраняем исторические котировки с тайм-фреймом 1 минута с 01 января 2023 по 01 января 2024 
   года в источник данных (DataFeed) `data`, присваивая ему имя `aflt`:

   ```python
   data = store.get_data(sec_id='AFLT', fromdate='01-01-2023', todate='01-01-2024', tf='1m')
   ```
   
   Все аргументы метода `get_data` являются обязательными:

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

   Метод `get_data` возвращает объект [feeds.PandasData](https://www.backtrader.ru/docu/datafeed/datafeed_pandas/) 
   экосистемы _backtrader_, поэтому его можно сразу подгружать в `cerebro` с помощью `cerebro.adddata()`.


4. Добавление Источника данных в движок [cerebro](https://www.backtrader.ru/docu/cerebro/cerebro/) осуществляется 
стандартно:

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
    data = store.get_data("AFLT", "01-01-2023", "01-01-2024", "1h")

    cerebro.adddata(data)
    cerebro.run()
    cerebro.plot(style="bar")


if __name__ == '__main__':
    runstrat()
```

Вывод покажет загруженный Источник данных:

![pict1.png](https://github.com/Celeevo/moex_store/blob/master/pict1.png?raw=true)

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
                              tf=tf)
        cerebro.adddata(data, name=tiker)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()
```

Источники данных, добавленные в `cerebro`:

![pict2.png](https://github.com/Celeevo/moex_store/blob/master/pict2.png?raw=true)








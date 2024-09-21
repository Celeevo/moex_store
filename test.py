from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from datetime import datetime, timedelta
from moex_store import MoexStore

spam = {'1m': 1, '5m': 5, '10m': 10, '15m': 15, '30m': 30, '1h': 60, '1d': 24, '1w': 7, '1M': 31, '1q': 4}

def runstrat():
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=True)

    for tiker in ('Siz4', 'Riz4'): # 'SiM4', 'RiH4', 'GZM4', 'RUABICP', 'RGBI', 'RUPCI'
        data = store.getdata(sec_id=tiker,
                             fromdate=datetime.today() - timedelta(days=1),
                             todate=datetime.today(),
                             tf='1m',
                             name='Смузи!')
        cerebro.adddata(data)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from datetime import datetime
from moex_store import MoexStore


def runstrat():
    d1 = datetime(2023, 5, 18, 11, 45, 00)
    d2 = datetime(2023, 5, 18, 0, 0, 00)
    print(d1>d2)

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=True)
    fd = "2024-03-21"
    # td = datetime.today()
    td = "2024-03-22"
    # tf='15m'
    tf='1h'
    # for tiker in ('SiM4', 'RiH4', 'GZM4', 'RUABICP', 'RGBI', 'RUPCI'):
    for tiker in ('SiM4', 'RiH4'):
        data = store.get_data(sec_id=tiker, fromdate=fd, todate=td, tf=tf)
        cerebro.adddata(data, name=tiker)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()

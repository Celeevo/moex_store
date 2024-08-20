from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from datetime import datetime
from moex_store import MoexStore


def runstrat():
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=True)
    fd = "13-12-2023"
    td = datetime.today()
    tf='15m'
    for tiker in ('SiM4', 'RiH4'):
        data = store.get_data(sec_id=tiker, fromdate=fd, todate=td, tf=tf)
        cerebro.adddata(data, name=tiker)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()

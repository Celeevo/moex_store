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
    td = '15-03-2024'
    tf='15m'
    today = datetime.today()
    for tiker in ('SiM4', 'EuH4', 'RiH4'):
        data = store.get_data(f'{tiker}_{tf}', tiker, fd, td, tf)
        cerebro.adddata(data)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()

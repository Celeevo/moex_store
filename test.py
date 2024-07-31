from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from datetime import datetime
from moex_store import MoexStore


def runstrat():
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=True)
    tf = '1d'
    today = datetime.today()
    for tiker in ('GAZP', 'NLMK', 'SIH4'):
        data = store.get_data(f'{tiker}_{tf}', tiker, '01-01-2023', today, tf)
        cerebro.adddata(data)

    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()

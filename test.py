from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from moex_store import MoexStore


def runstrat():

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)
    store = MoexStore(write_to_file=True)
    # data = store.get_data('EuM4', 'EuM4', "2024-03-15", "2024-05-29", '1m')
    data = store.get_data('', 'AFLT', "2022-01-15", "2022-11-15", '1m')
    # data = store.get_data('', 'NLMK', "2022-01-15", "2022-11-15", '1h')
    # data = store.get_data('', 'SIz3', "2022-01-15", "2022-11-15", '1h')
    cerebro.adddata(data)
    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()
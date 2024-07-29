from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
from moex_store import MoexStore

TF = {
    '1m': 1,
    '5m': 5,
    '10m': 10,
    '15m': 15,
    '30m': 30,
    '1h': 60,
    '1d': 24,
    '1w': 7,
    '1M': 31,
    '1q': 4
}

def runstrat():

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.addstrategy(bt.Strategy)

    store = MoexStore(write_to_file=True)
    for tf in ('1d', '1w'):  # '1m', '10m', '1h',
        for fd, td in [('01-01-2001', '01-03-2001'),
                       # ('01-01-2015', '01-05-2015'),
                       # ('01-01-2015', '01-07-2015'),
                       # ('01-01-2015', '01-09-2015'),
                       # ('01-01-2015', '01-11-2015'),
                       # ('01-01-2015', '01-01-2016'),
                       # ('01-01-2015', '01-01-2017'),
                       # ('01-01-2015', '01-01-2018')
                       ]:
            data = store.get_data('', 'AFLT', fd, td, tf)
    # data = store.get_data('', 'NLMK', "2022-01-15", "2022-11-15", '1h')
    # data = store.get_data('', 'SIz3', "2022-01-15", "2022-11-15", '1h')
    cerebro.adddata(data)
    cerebro.run()
    cerebro.plot(style='bar')


if __name__ == '__main__':
    runstrat()
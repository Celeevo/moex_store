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


# def runstrat():
#     cerebro = bt.Cerebro(stdstats=False)
#     cerebro.addstrategy(bt.Strategy)
#
#     store = MoexStore(write_to_file=True)
#     for tf in ('1m', '10m', '1h', '1d', '1w'):  # '1m', '10m', '1h',
#         for fd, td in [
#             # ('01-01-2015', '01-03-2015'),
#             # ('01-01-2015', '01-05-2015'),
#             # ('01-01-2015', '01-07-2015'),
#             # ('01-01-2015', '01-09-2015'),
#             # ('01-01-2015', '01-11-2015'),
#             ('01-01-2023', '01-01-2024'),
#             # ('01-01-2015', '01-01-2017'),
#             # ('01-01-2015', '01-01-2018')
#         ]:
#             data = store.get_data(f'AFLT {tf=} from "{fd}" to "{td}"', 'AFLT', fd, td, tf)
#             # data = store.get_data('', 'NLMK', "2022-01-15", "2022-11-15", '1h')
#             # data = store.get_data('', 'SIz3', "2022-01-15", "2022-11-15", '1h')
#             cerebro.adddata(data)
#     cerebro.run()
#     cerebro.plot(style='bar')
#
#
# if __name__ == '__main__':
#     runstrat()


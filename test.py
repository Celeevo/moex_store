from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from datetime import datetime
from datetime import timedelta

import backtrader as bt
from moex_store import MoexStore
# from sizers.strategy_lib_order_diff import RsiStrategy, comminfo, UniRiskSizer
# from sizers.strategy_lib_no_sizer import RsiStrategy, comminfo, UniRiskSizer
# import datetime

def runstrat():
    contracts = ['EuZ1', 'EuZ3', 'EuH4', 'EuM4', 'EuU4']
    datas = []
    for sec_id in contracts:
        # sec_id = 'EuZ3'
        tf = '15m'
        store = MoexStore()
        fromdate = store.futures.prevexpdate(sec_id)
        todate = store.futures.expdate(sec_id)
        # data = store.getdata(sec_id=sec_id,
        datas.append(store.getdata(sec_id=sec_id,
                             fromdate=fromdate,
                             todate=todate,
                             tf=tf,
                             name=sec_id))
    # for data in datas:
    #     cerebro = bt.Cerebro()
    #     cerebro.broker = bt.brokers.BackBroker()
    #     cerebro.broker.setcash(100000)
    #     cerebro.adddata(data)
    #     cerebro.broker.addcommissioninfo(comminfo)
    #     cerebro.addsizer(UniRiskSizer)
    #     cerebro.addstrategy(RsiStrategy, rsi_period=12, all_in=True, solid_exit=True, risk=12, log=False)
    #     # cerebro.optstrategy(RsiStrategy, rsi_period=(12, ), stop_period=(7, ), slippage=range(0,16))
    #     cerebro.run()
        # cerebro.plot(style='bar', linevalues=False, valuetags=False)
        # cerebro.plot(style='bar', start=datetime.date(2024, 3, 26), end=datetime.date(2024, 3, 28))


if __name__ == '__main__':
    runstrat()

# spam = {'1m': 1, '5m': 5, '10m': 10, '15m': 15, '30m': 30, '1h': 60, '1d': 24, '1w': 7, '1M': 31, '1q': 4}


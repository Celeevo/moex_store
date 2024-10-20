from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from datetime import datetime
from datetime import timedelta

import backtrader as bt
from moex_store import MoexStore
from sizers.strategy_lib_no_sizer import RsiStrategy, comminfo, UniRiskSizer
# import datetime

def runstrat():
    contracts = ['EuH3', 'EuM3', 'EuU3', 'EuZ3', 'EuH4', 'EuM4', 'EuU4']
    datas = []
    pnl, profit_trades, losing_trades = 0, 0, 0
    for sec_id in contracts:
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
    #     cerebro.addstrategy(RsiStrategy, rsi_period=12, all_in=True, solid_exit=False, risk=4, log=False)
    #     # cerebro.optstrategy(RsiStrategy, rsi_period=(12, ), stop_period=(7, ), slippage=range(0,16))
    #     strat = cerebro.run()
    #
    #     strategy_instance = strat[0]
    #     pnl += strategy_instance.pnl
    #     profit_trades += strategy_instance.profit_trades
    #     losing_trades += strategy_instance.losing_trades
    #
    #     # cerebro.plot(style='bar', linevalues=False, valuetags=False)
    #     # cerebro.plot(style='bar', start=datetime.date(2024, 3, 26), end=datetime.date(2024, 3, 28))
    # pl_ratio = round(profit_trades/losing_trades, 2)
    # print(f'{50*"-"} \n Итого Профит = {pnl}, Прибыльные сделки = {profit_trades}, Убыточные сделки = {losing_trades}, P/L = {pl_ratio}')

if __name__ == '__main__':
    runstrat()
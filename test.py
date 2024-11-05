from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from datetime import datetime
from datetime import timedelta

import backtrader as bt
from moex_store import MoexStore
# import datetime

def runstrat():
    contracts = ['EuH3', 'EuM3', 'EuU3', 'EuZ3', 'EuH4', 'EuM4', 'EuU4']
    datas = []
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


if __name__ == '__main__':
    runstrat()
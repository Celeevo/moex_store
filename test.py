from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from moex_store import MoexStore


def runstrat():
    # Тестовая цепочка фьючерсов EUR/RUB. Для каждого контракта берем период
    # от экспирации предыдущего контракта до экспирации текущего.
    contracts = ['EuH3', 'EuM3', 'EuU3', 'EuZ3', 'EuH4', 'EuM4', 'EuU4']
    datas = []

    # Важно создавать один MoexStore на весь прогон: внутри него переиспользуются
    # кэши метаданных, признак уже выполненной проверки MOEX и текущий DNS resolver.
    store = MoexStore()
    for sec_id in contracts:
        tf = '15m'

        # prevexpdate и expdate - это даты экспирации контрактов, а не даты из имени CSV.
        # Эти даты формируют интервал котировок, который будет сохранен в files_from_moex.
        fromdate = store.futures.prevexpdate(sec_id)
        todate = store.futures.expdate(sec_id)
        datas.append(store.getdata(sec_id=sec_id,
                             fromdate=fromdate,
                             todate=todate,
                             tf=tf,
                             name=sec_id))


if __name__ == '__main__':
    runstrat()

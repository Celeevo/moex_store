from collections import OrderedDict

TF = OrderedDict({
    # '': 'тики',
    1: '1 мин',
    5: '5 мин', # NO
    10: '10 мин',
    15: '15 мин', # NO
    30: '30 мин', # NO
    60: '1 час',
    24: '1 день',
    7: '1 неделя',
    31: '1 месяц',
    4: '1 квартал'
})

PERS = OrderedDict({
    '': '0',
    1: '1',
    5: '5', # NO
    10: '10',
    15: '15', # NO
    30: '30', # NO
    60: '60',
    24: 'D',
    7: 'W',
    31: 'M',
    4: 'Q'
})


DATE_FORMAT = {
    '%Y%m%d': 'ггггммдд',
    '%y%m%d': 'ггммдд',
    '%d%m%y': 'ддммгг',
    '%d/%m/%y': 'дд/мм/гг',
    '%m/%d/%y': 'мм/дд/гг'
}

TIME_FORMAT = {
    '%H%M%S': 'ччммсс',
    '%H%M': 'ччмм',
    '%H:%M:%S': 'чч:мм:сс',
    '%H:%M': 'чч:мм'
}

FIELD_SEPARATOR = {
    ',': "запятая (,)",
    '.': "точка (.)",
    ';': "точка с запятой (;)",
    '\t': "табуляция (Tab)",
    ' ': "пробел ( )"
}

DIGIT_SEPARATOR = {
    "нет": "нет",
    ',': "запятая (,)",
    '.': "точка (.)",
    ' ': "пробел ( )",
    "'": "кавычка (')"
}

RECORD_FORMAT = [
        "TICKER, PER, DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL",
        "TICKER, PER, DATE, TIME, OPEN, HIGH, LOW, CLOSE",
        "TICKER, PER, DATE, TIME, CLOSE, VOL",
        "TICKER, PER, DATE, TIME, CLOSE",
        "DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL",
        # "TICKER, PER, DATE, TIME, LAST, VOL",
        # "TICKER, DATE, TIME, LAST, VOL",
        # "TICKER, DATE, TIME, LAST",
        # "DATE, TIME, LAST, VOL",
        # "DATE, TIME, LAST",
        # "DATE, TIME, LAST, VOL, ID",
        # "DATE, TIME, LAST, VOL, ID, OPER"
]

import aiomoex
import asyncio
import pandas as pd
from pandas import DataFrame
import aiohttp
from aiomoex import client, request_helpers
from collections.abc import Iterable
from typing import Final
SECURITIES: Final = "securities"


async def get_instrument_info(secid):
    async with aiohttp.ClientSession() as session:
        url = f"https://iss.moex.com/iss/securities/{secid}.json"
        async with session.get(url) as response:
            data = await response.json()

            if 'boards' in data and 'data' in data['boards']:
                boards_data = data['boards']['data']
                columns = data['boards']['columns']
                for item in boards_data:
                    record = dict(zip(columns, item))
                    board = record.get('boardid')
                    market = record.get('market')
                    engine = record.get('engine')
                    is_primary = record.get('is_primary')
                    if is_primary:
                        print(f"secid: {secid}, board: {board}, market: {market}, engine: {engine}")
            else:
                print(f"No data found for secid: {secid}")


# Замените 'GAZP' на ваш secid
secid = 'GAZP'
asyncio.run(get_instrument_info(secid))

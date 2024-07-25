import aiohttp
import asyncio
import aiomoex
import patch_aiohttp  # Патч для macos


async def get_instrument_info(secid):
    async with aiohttp.ClientSession() as session:
        url = f"https://iss.moex.com/iss/securities/{secid}.json"
        async with session.get(url) as response:
            data = await response.json()

            # последнее условие проверяет не пустой ответ по инструменту,
            # если его вообще нет на бирже
            if 'boards' in data and 'data' in data['boards'] and data['boards']['data']:
                boards_data = data['boards']['data']
                columns = data['boards']['columns']

                # Инструмент может торговаться на многих бордах, выбираем главный is_primary =1
                primary_boards = filter(lambda item: dict(zip(columns, item)).get('is_primary') == 1, boards_data)

                for item in primary_boards:
                    record = dict(zip(columns, item))
                    board = record.get('boardid')
                    market = record.get('market')
                    engine = record.get('engine')
                    print(f"secid: {secid}, board: {board}, market: {market}, engine: {engine}")
                    response = secid, board, market, engine
                    return response
            else:
                print(f"No data found for secid: {secid}")
                return None


async def get_history_intervals(sec_id, board, market, engine):
    async with aiohttp.ClientSession() as session:
        data = await aiomoex.get_market_candle_borders(session, security=sec_id, market=market, engine=engine)
        if data:
            print(f'Для {sec_id} Интервалы есть на market')
            return 'm', data

        data = await aiomoex.get_board_candle_borders(session, security=sec_id, board=board, market=market, engine=engine)
        if data:
            print(f'Для {sec_id} Интервалы есть ТОЛЬКО на board')
            return 'b', data

        print(f'Для {sec_id} нет Интервалов на Бирже')
        return 'n', None

# Замените 'GAZP' на ваш secid 'Rim4' 'FDEXW1WHT391'
secid = "GAZP"
response = asyncio.run(get_instrument_info(secid))
if response:
    context, data = asyncio.run(get_history_intervals(*response))

    print(data)

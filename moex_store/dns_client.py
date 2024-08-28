import aiohttp
from aiohttp import client_exceptions
import socket
from aiomoex.client import ISSClient, ISSMoexError


class DNS_ISSClient(ISSClient):
    async def get(self, start=None):
        """Загрузка данных.

        :param start:
            Номер элемента с которого нужно загрузить данные. Используется для дозагрузки данных,
            состоящих из нескольких блоков. При отсутствии данные загружаются с начального элемента.

        :return:
            Блок данных с отброшенной вспомогательной информацией - словарь, каждый ключ которого
            соответствует одной из таблиц с данными. Таблицы являются списками словарей, которые напрямую
            конвертируются в pandas.DataFrame.
        :raises ISSMoexError:
            Ошибка при обращении к ISS Moex.
        """
        url = self._url
        query = self._make_query(start)
        # print(f"{datetime.now()}: DNS_ISSClient.get.url: {url}")
        for dns_server in ['77.88.8.1', '8.8.8.8']:
            connector = None
            try:
                # import ssl  # start
                # ssl_context = ssl.create_default_context()
                # ssl_context.check_hostname = False
                # ssl_context.verify_mode = ssl.CERT_NONE  # end

                resolver = aiohttp.resolver.AsyncResolver(nameservers=[dns_server])
                connector = aiohttp.TCPConnector(resolver=resolver)  # , ssl=ssl_context
                self._session._connector = connector
                async with self._session.get(url, params=query) as response:
                    try:
                        response.raise_for_status()
                    except client_exceptions.ClientResponseError as err:
                        raise ISSMoexError("Неверный url", result.url) from err
                    else:
                        result = await response.json()
                        return result[1]
            except (aiohttp.ClientError, socket.gaierror) as e:
                print(f"Failed to make a request to '{url}' using DNS server '{dns_server}'. Error: {e}")
                continue
            finally:
                if connector and not connector.closed:
                    await connector.close()
        return None


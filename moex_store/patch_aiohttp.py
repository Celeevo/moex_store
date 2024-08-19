import platform
import aiomoex
import asyncio
from moex_store.dns_client import DNS_ISSClient

# print(f'OS = {platform.system()}')
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

aiomoex.client.ISSClient = DNS_ISSClient

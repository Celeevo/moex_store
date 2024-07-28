import aiohttp
import ssl




# Создаем SSL-контекст с отключенной проверкой сертификатов
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Переопределяем оригинальный метод ClientSession
_original_init = aiohttp.ClientSession.__init__


def _patched_init(self, *args, **kwargs):
    if 'connector' not in kwargs:
        kwargs['connector'] = aiohttp.TCPConnector(ssl=ssl_context)
    _original_init(self, *args, **kwargs)


aiohttp.ClientSession.__init__ = _patched_init
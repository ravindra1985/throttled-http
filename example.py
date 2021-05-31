import logging
import sys

from concurrent_http import ConcurrentHttp

_LOGGER = logging.getLogger()


class BulkPatch(ConcurrentHttp):
    async def execute(self, pos, record):
        url = self.config.get('read_url')
        entity = await self.read(url.format(record))
        _LOGGER.info(f"{pos} => response: {entity}")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    config_ = {
        'data_file': './data.txt',
        'concurrency': 3,
        'read_url': 'http://localhost:8090/{}'
    }
    BulkPatch(config=config_).run()

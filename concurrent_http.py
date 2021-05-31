import abc
import asyncio
import logging
import sys

import aiohttp
import backoff
import ujson
import uvloop
from aiohttp import ClientResponseError, ClientError

_LOGGER = logging.getLogger()


class ConcurrentHttp(object):
    __metaclass__ = abc.ABCMeta
    __author__ = 'Ravindra Mulakala'

    def __init__(self, config=None, loop=None):
        self.config = config or {}
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = loop or asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.queue = asyncio.Queue()
        self.source = config.get('data_file')
        self.poll_interval = config.get('poll_interval') or 1
        self.concurrency = config.get('concurrency') or 1
        self.high_watermark = config.get('high_watermark') or (1000 * self.concurrency)
        self.session = None
        assert self.concurrency > 0, _LOGGER.error('concurrency should be 1 or higher')
        assert self.poll_interval > 0, _LOGGER.error('poll_interval should be 1 or higher')
        assert self.source is not None, _LOGGER.error('provide file with multiple lines')

    async def _iterate_source(self, worker_name):
        with open(self.source, 'r') as reader:
            pos = 1
            for line in reader:
                while self.queue.qsize() > self.high_watermark:  # poll queue for its capacity
                    _LOGGER.debug(f'{worker_name} => queue size({self.queue.qsize()}) > high water mark({self.high_watermark}), sleeping for a moment')
                    await asyncio.sleep(self.poll_interval)  # sleep while queue getting drained
                self.queue.put_nowait((pos, line.strip()))  # keep adding items until high water mark is reached
                pos += 1

    async def _worker_task(self):
        while True:
            pos, record = await self.queue.get()
            await self.execute(pos, record)  # application logic to be implemented in child class
            self.queue.task_done()

    @abc.abstractmethod
    async def execute(self, pos, record):
        _LOGGER.error('implement task')

    async def _launch_workers(self):
        workers = [asyncio.create_task(self._iterate_source('poller'))]  # polls queue and add tasks based on high water mark without filling the buffer
        for worker in range(self.concurrency):
            workers.append(asyncio.create_task(self._worker_task()))
        await asyncio.sleep(self.poll_interval)  # wait for poll_queue to initial items, otherwise workers will have nothing to do and cancels themselves
        await self.queue.join()
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def _create_session(self):
        trace_config = aiohttp.TraceConfig()

        async def on_request_start(session, context, params):
            context.on_request_start = asyncio.get_event_loop().time()
            context.is_redirect = False

        async def on_request_end(session, context, params):
            total = asyncio.get_event_loop().time() - context.on_request_start
            context.on_request_end = total
            _LOGGER.debug(f'time taken: {total}')

        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        self.session = aiohttp.ClientSession(trace_configs=[trace_config], json_serialize=ujson.dumps)

    async def _close_all(self):
        await self.session.close()

    @backoff.on_exception(backoff.expo, (asyncio.TimeoutError, ClientResponseError, ClientError), max_tries=3, max_time=60)
    async def read(self, url):
        ctx = {'handle': url}
        async with self.session.get(url, timeout=30, raise_for_status=False, trace_request_ctx=ctx) as response:
            if response.status == 200:
                result = await response.json()  # no need to assert for 200 status as back-off will throw error
                return result  # assert response.status == 200
            else:
                return None

    @backoff.on_exception(backoff.expo, (asyncio.TimeoutError, ClientResponseError, ClientError), max_tries=3, max_time=60)
    async def update(self, url, payload, ):
        async with self.session.put(url, json=payload, timeout=30, raise_for_status=True, trace_request_ctx={}) as response:
            return response

    @backoff.on_exception(backoff.expo, (asyncio.TimeoutError, ClientResponseError, ClientError), max_tries=3, max_time=60)
    async def delete(self, url):
        async with self.session.delete(url, timeout=30, raise_for_status=True, trace_request_ctx={}) as response:
            return response

    def run(self):
        _LOGGER.info(f'running with, concurrency:{self.concurrency}, buffer-high-watermark:{self.high_watermark}, poll-interval:{self.poll_interval}')
        self.loop.run_until_complete(self._create_session())
        self.loop.run_until_complete(self._launch_workers())
        self.loop.run_until_complete(self._close_all())
        _LOGGER.info(f'all done!')

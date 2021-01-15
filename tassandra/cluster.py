import collections
import numbers

from tornado.concurrent import Future

from tassandra.pool import Pool
from tassandra.request import Request


DEFAULT_TIMEOUT = (0.5,)


class Cluster:
    def __init__(self, contact_points=('127.0.0.1',), port=9042, statsd_client=None):
        if contact_points is not None:
            if isinstance(contact_points, str):
                raise TypeError('contact_points should not be a string, it should be a sequence (e.g. list) of strings')

        self.pool = Pool(contact_points, port, statsd_client)

    async def init(self):
        await self.pool.init()

    def close(self):
        self.pool.close()

    def execute(self, query, timeout=DEFAULT_TIMEOUT):
        if not isinstance(timeout, collections.Iterable):
            timeout = (timeout,)
        for t in timeout:
            if not isinstance(t, numbers.Number):
                raise TypeError('Timeout should be number')

        future = Future()
        self.pool.execute(Request(query, future, timeout))
        return future

# coding=utf-8

import collections
import numbers

from tornado.ioloop import IOLoop
from tornado.concurrent import Future

from tassandra.compat import string_types
from tassandra.pool import Pool
from tassandra.request import Request


DEFAULT_TIMEOUT = (0.5,)


class Cluster(object):
    def __init__(self, contact_points=('127.0.0.1',), port=9042):
        if contact_points is not None:
            if isinstance(contact_points, string_types):
                raise TypeError('contact_points should not be a string, it should be a sequence (e.g. list) of strings')

        self.pool = Pool(contact_points, port, IOLoop.instance())

    def execute(self, query, timeout=DEFAULT_TIMEOUT):
        if not isinstance(timeout, collections.Iterable):
            timeout = (timeout,)
        for t in timeout:
            if not isinstance(t, numbers.Number):
                raise TypeError('Timeout should be number')

        future = Future()
        self.pool.execute(Request(query, future, timeout))
        return future

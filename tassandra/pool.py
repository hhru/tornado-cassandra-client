# coding=utf-8

import collections
import functools
import logging
import random

from cassandra.query import named_tuple_factory

from tassandra.connection import Connection, RequestTimeout


log = logging.getLogger('tassandra.pool')


CONSECUTIVE_ERRORS_LIMIT = 500


class Pool(object):
    def __init__(self, contact_points, port, io_loop, statsd_client=None):
        self.io_loop = io_loop
        self.queue = collections.deque()
        self.queries = {}
        self.num_connection = len(contact_points)
        self.connections = []
        self.status_mask = 0

        identifier = 1
        for contact_point in contact_points:
            self.connections.append(
                Connection(identifier, contact_point, port, self.io_loop, self.connection_status_callback))
            identifier <<= 1
        self.statsd_client = statsd_client
        log.info('connection pool to %s initialized', contact_points)

    def connection_status_callback(self, identifier, status):
        if status:
            self.status_mask |= identifier
            self.io_loop.add_callback(self._process_queue)
        else:
            self.status_mask &= ~identifier

    def is_alive(self, identifier):
        return self.status_mask & identifier != 0

    def result_callback(self, key, message):
        if key not in self.queries:
            return

        request = self.queries[key]
        del self.queries[key]

        if key in self.queue:
            self.queue.remove(key)

        request.register_response(self.io_loop, message)
        self._log_stats(request)

        if request.failed:
            self._increase_consecutive_errors(request)

            if request.is_retry_possible():
                self.execute(request)
            else:
                message.message += '(' + str(request) + ')'
                request.future.set_exception(message)
        else:
            self._decrease_consecutive_errors(request)

            if isinstance(message.results, tuple):
                result = named_tuple_factory(*message.results)
            else:
                result = message.results

            request.future.set_result(result)

    def execute(self, request):
        key = object()

        request.add_timeout(self.io_loop, functools.partial(self._on_timeout, key))

        self.queue.append(key)
        self.queries[key] = request
        self._process_queue()

        if self.queue:
            log.debug('request queued. %d queued requests.', len(self.queue))

    def _process_queue(self):
        while self.queue and self.status_mask != 0:
            key = self.queue.popleft()
            request = self.queries[key]
            connection = self.get_connection(request.used_connections_bitmap)
            request.send(connection, functools.partial(self.result_callback, key))

    def get_connection(self, used_connections_bitmap):
        possible = self.status_mask & ~ used_connections_bitmap
        if possible == 0:
            possible = self.status_mask

        ready = [index for index, val in enumerate(bin(possible)[::-1]) if val == '1']
        return self.connections[ready[random.randint(0, len(ready) - 1)]]

    def _on_timeout(self, key):
        self.result_callback(key, RequestTimeout())

    def _increase_consecutive_errors(self, request):
        if request.current_connection is None:
            return

        connection = request.current_connection
        connection.consecutive_errors += 1
        if connection.consecutive_errors > CONSECUTIVE_ERRORS_LIMIT and self.is_alive(connection.identifier):
            log.info('closing connection to %s due to consecutive errors limit exceeded', connection.host)
            connection.close()

    def _log_stats(self, request):
        if self.statsd_client and request is not None:
            self.statsd_client.count('cassandra.requests', 1,
                                     server=request.current_connection.host,
                                     already_tried=request.tries,
                                     failed='true' if request.failed else 'false')

    @staticmethod
    def _decrease_consecutive_errors(request):
        if request.current_connection is None:
            return

        request.current_connection.consecutive_errors = 0

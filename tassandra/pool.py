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
    def __init__(self, contact_points, port, io_loop):
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

        if request.timeout_handler is not None:
            self.io_loop.remove_timeout(request.timeout_handler)
            request.timeout_handler = None

        if isinstance(message, Exception):
            self._increase_consecutive_errors(request)

            if request.check_retry():
                self.execute(request)
            else:
                message.message += '(' + str(request) + ')'
                request.future.set_exception(message)
        else:
            self._decrease_consecutive_errors(request)
            request.future.set_result(named_tuple_factory(*message.results))

    def execute(self, request):
        key = object()

        request.timeout_handler = self.io_loop.add_timeout(self.io_loop.time() + request.get_current_timeout(),
                                                           functools.partial(self._on_timeout, key))

        self.queue.append(key)
        self.queries[key] = request
        self._process_queue()

        if self.queue:
            log.debug('request queued. %d queued requests.', len(self.queue))

    def _process_queue(self):
        while self.queue and self.status_mask != 0:
            key = self.queue.popleft()
            request = self.queries[key]

            connection = self.get_connection(request.tried)
            request.tried |= connection.identifier
            request.current_connection = connection
            connection.send_msg(request.query, functools.partial(self.result_callback, key))

    def get_connection(self, tried):
        possible = self.status_mask & ~ tried
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

    def _decrease_consecutive_errors(self, request):
        if request.current_connection is None:
            return

        request.current_connection.consecutive_errors = 0

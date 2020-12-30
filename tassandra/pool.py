import asyncio
import collections
import functools
import logging
import random

from cassandra.query import named_tuple_factory
from tornado.ioloop import IOLoop

from tassandra.connection import Connection, RequestTimeoutException, ConnectionShutdownException

log = logging.getLogger('tassandra.pool')


CONSECUTIVE_ERRORS_LIMIT = 500


class Pool:
    def __init__(self, contact_points, port, statsd_client=None):
        self.closed = False
        self.queue = collections.deque()
        self.queries = {}
        self.num_connection = len(contact_points)
        self.connections = []
        self.status_mask = 0
        self.request_tasks = []

        identifier = 1
        for contact_point in contact_points:
            self.connections.append(
                Connection(identifier, contact_point, port, self.connection_status_callback))
            identifier <<= 1
        self.statsd_client = statsd_client
        log.info('connection pool to %s initialized', contact_points)

    async def init(self):
        await asyncio.gather(*[connection.connect() for connection in self.connections])

    def close(self):
        self.closed = True
        for connection in self.connections:
            connection.close()

        for request in self.queries.values():
            request.future.set_exception(ConnectionShutdownException())

        self.queries.clear()
        self.queue.clear()

        [task.cancel() for task in self.request_tasks if not task.done()]
        self.request_tasks = []

    def connection_status_callback(self, identifier, status):
        if status:
            self.status_mask |= identifier
            IOLoop.current().add_callback(self._process_queue)
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

        request.register_response(message)
        self._log_stats(request)

        if request.failed:
            self._increase_consecutive_errors(request)

            if request.is_retry_possible():
                self.execute(request)
            else:
                if isinstance(message, ConnectionShutdownException) or isinstance(message, RequestTimeoutException):
                    message.request = request
                request.future.set_exception(message)
        else:
            self._decrease_consecutive_errors(request)

            if message.schema_change_event is not None:
                result = message.schema_change_event
            elif message.parsed_rows is not None:
                result = named_tuple_factory(message.column_names, message.parsed_rows)
            else:
                result = None

            request.future.set_result(result)

    def execute(self, request):
        if self.closed:
            request.future.set_exception(ConnectionShutdownException())
            return

        key = object()

        request.add_timeout(functools.partial(self._on_timeout, key))

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
            self.request_tasks.append(
                asyncio.ensure_future(request.send(connection, functools.partial(self.result_callback, key))))

    def get_connection(self, used_connections_bitmap):
        possible = self.status_mask & ~ used_connections_bitmap
        if possible == 0:
            possible = self.status_mask

        ready = [index for index, val in enumerate(bin(possible)[::-1]) if val == '1']
        return self.connections[ready[random.randint(0, len(ready) - 1)]]

    def _on_timeout(self, key):
        self.result_callback(key, RequestTimeoutException())

    def _increase_consecutive_errors(self, request):
        if request.current_connection is None:
            return

        connection = request.current_connection
        connection.consecutive_errors += 1
        if connection.consecutive_errors > CONSECUTIVE_ERRORS_LIMIT and self.is_alive(connection.identifier):
            log.info('closing connection to %s due to consecutive errors limit exceeded', connection.host)
            connection.reconnect()

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

# coding=utf-8

import socket
import logging
from collections import deque
from functools import wraps

from tornado.iostream import IOStream

from cassandra.protocol import decode_response, StartupMessage, ReadyMessage, ErrorMessage
from cassandra.marshal import v3_header_unpack, int32_unpack


HEADER_LENGTH = 5
FULL_HEADER_LENGTH = 9
DEFAULT_CQL_VERSION = '3.0.0'
DEFAULT_CQL_VERSION_NUMBER = 3
RECONNECT_TIMEOUT = 0.2
MAXIMUM_RECONNECT_TIMEOUT = 5


log = logging.getLogger('tassandra.connection')


class ConnectionShutdown(Exception):
    def __str__(self):
        return 'Connection Shutdown Exception: ' + self.message


class RequestTimeout(Exception):
    def __str__(self):
        return 'Request Timeout Exception: ' + self.message


def close_on_error(callback):
    @wraps(callback)
    def wrapper(self, *args, **kwargs):
        try:
            return callback(self, *args, **kwargs)
        except Exception:
            log.exception('unhandled exception in callback, close connection')
            self.close()

    return wrapper


class Connection(object):
    def __init__(self, identifier, host, port, io_loop, status_callback):
        self.identifier = identifier
        self.io_loop = io_loop
        self.host = host
        self.port = port
        self.status_callback = status_callback
        self.max_request_id = (2 ** 15) - 1
        self.reconnect_timeout = RECONNECT_TIMEOUT
        self._header = None

        self._connect()

    def get_request_id(self):
        try:
            return self.request_ids.popleft()
        except IndexError:
            self.highest_request_id += 1
            assert self.highest_request_id <= self.max_request_id
            return self.highest_request_id

    def close(self):
        log.info('connection to %s closed.', self.host)

        self.status_callback(self.identifier, False)

        if not self.stream.closed():
            self.stream.set_close_callback(None)
            self.stream.close()

        for callback in self._callbacks.itervalues():
            callback(ConnectionShutdown())

        log.debug('reconnect in %f', self.reconnect_timeout)
        self.io_loop.add_timeout(self.io_loop.time() + self.reconnect_timeout, self._connect)
        self.reconnect_timeout = min(self.reconnect_timeout * 2, MAXIMUM_RECONNECT_TIMEOUT)

    @close_on_error
    def body_cb(self, body):
        version, flags, stream_id, opcode = v3_header_unpack(self._header)

        if not self.request_ids and self.highest_request_id >= self.max_request_id:
            self.status_callback(self.identifier, True)

        self.request_ids.append(stream_id)

        response = decode_response(DEFAULT_CQL_VERSION_NUMBER, None, stream_id, flags, opcode, body)
        self._callbacks[stream_id](response)
        self.stream.read_bytes(FULL_HEADER_LENGTH, self.header_cb)

    @close_on_error
    def header_cb(self, header):
        self._header = header[:HEADER_LENGTH]

        body_len = int32_unpack(header[-4:])
        self.stream.read_bytes(body_len, self.body_cb)

    @close_on_error
    def connected_callback(self):
        self.stream.read_bytes(FULL_HEADER_LENGTH, self.header_cb)

        sm = StartupMessage(cqlversion=DEFAULT_CQL_VERSION, options={})
        self.send_msg(sm, cb=self._handle_startup_response)

    @close_on_error
    def _handle_startup_response(self, message):
        if isinstance(message, ReadyMessage):
            log.debug('got ReadyMessage on connection from %s', self.host)
            self.reconnect_timeout = RECONNECT_TIMEOUT
            self.status_callback(self.identifier, True)
            log.info('connection to %s established', self.host)
        elif isinstance(message, ErrorMessage):
            log.info('closing connection to %s due to startup error: %s', self.host, message.summary_msg())
            self.close()
        elif isinstance(message, ConnectionShutdown):
            log.debug('connection to %s was closed during the startup handshake', self.host)
        else:
            log.error('closing connection to %s due to unexpected response during startup: %r', self.host, message)
            self.close()

    def _connect(self):
        log.debug('attempt to connect to %s.', self.host)

        self.request_ids = deque(range(300))
        self.highest_request_id = 299
        self._callbacks = {}
        self.consecutive_errors = 0

        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)

        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 1)
        except AttributeError:
            log.debug('socket doesn\'t seem to support TCP_KEEPIDLE option')

        try:
            setattr(socket, 'TCP_USER_TIMEOUT', 18)
            sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, 'TCP_USER_TIMEOUT'), 5000)
        except socket.error:
            log.debug('this kernel doesn\'t seem to support TCP_USER_TIMEOUT')

        self.stream = IOStream(sock, io_loop=self.io_loop)
        self.stream.connect((self.host, self.port), self.connected_callback)
        self.stream.set_close_callback(self.close)

    def send_msg(self, query, cb):
        if self.stream.closed():
            cb(ConnectionShutdown())
            return

        request_id = self.get_request_id()

        if request_id >= self.max_request_id:
            self.status_callback(self.identifier, False)

        self._callbacks[request_id] = cb
        self.stream.write(query.to_binary(request_id, DEFAULT_CQL_VERSION_NUMBER))

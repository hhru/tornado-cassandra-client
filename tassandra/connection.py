import asyncio
import socket
import logging
import time
from collections import deque

from tornado.iostream import IOStream

from cassandra.protocol import ProtocolHandler, StartupMessage, ReadyMessage, ErrorMessage
from cassandra.marshal import v3_header_unpack, int32_unpack


HEADER_LENGTH = 5
FULL_HEADER_LENGTH = 9
DEFAULT_CQL_VERSION = '4.0.0'
DEFAULT_CQL_VERSION_NUMBER = 4
RECONNECT_TIMEOUT = 0.2
MAXIMUM_RECONNECT_TIMEOUT = 5


log = logging.getLogger('tassandra.connection')


class ConnectionShutdownException(Exception):

    def __init__(self, host=None, request=None):
        self.host = host
        self.request = request

    def __str__(self):
        return 'Connection to {} closed ({})'.format(self.host, self.request)


class RequestTimeoutException(Exception):

    def __init__(self, request=None):
        self.request = request

    def __str__(self):
        return 'Request Timeout Exception: {}'.format(self.request)


class StartupException(Exception):
    pass


class StartupException2(Exception):
    pass


class StartupException3(Exception):
    pass


class Connection:
    """
    Протокол кассандры полностью асинхронный. Клиент каждому запросу присваивает
    уникальный в пределах подключения stream_id, а сервер когда-нибудь, возможно, пришлет ответ с этим же id.
    Поэтому для чтения мы просто ждем, когда придет какой то хедер, потом ждем тела,
    а потом снова ждем любой хедер возможного следующего ответа.
    """
    def __init__(self, identifier, host, port, status_callback):
        self.identifier = identifier
        self.host = host
        self.port = port
        self.status_callback = status_callback
        self.max_request_id = (2 ** 15) - 1
        self.reconnect_timeout = RECONNECT_TIMEOUT
        self._header = None
        self.recursively_read_task = None

    async def connect(self):
        self._connection_prepare()

        try:
            await self.stream.connect((self.host, self.port))

            await self.send_msg(
                StartupMessage(cqlversion=DEFAULT_CQL_VERSION, options={}),
                self._handle_startup_response
            )

        except Exception as e:
            log.exception(f'unhandled exception during connection {type(e).__name__}, close connection')
            asyncio.ensure_future(self.reconnect())

        else:
            self.recursively_read_task = asyncio.ensure_future(self.recursively_read())

    def get_request_id(self):
        try:
            return self.request_ids.popleft()
        except IndexError:
            self.highest_request_id += 1
            assert self.highest_request_id <= self.max_request_id
            return self.highest_request_id

    def close(self):
        self.status_callback(self.identifier, False)

        if not self.stream.closed():
            self.stream.close()

        for callback in iter(self._callbacks.values()):
            callback(ConnectionShutdownException(host=self.host))

        if self.recursively_read_task:
            self.recursively_read_task.cancel()
            self.recursively_read_task = None

    async def reconnect(self):
        log.info('connection to %s closed.', self.host)

        self.close()

        log.debug('reconnect in %f', self.reconnect_timeout)
        await asyncio.sleep(self.reconnect_timeout)
        await self.connect()
        self.reconnect_timeout = min(self.reconnect_timeout * 2, MAXIMUM_RECONNECT_TIMEOUT)

    async def recursively_read(self):
        while True:
            try:
                body_length = await self.read_header()
                await self.read_body(body_length)
            except Exception as e:
                log.exception(f'unhandled exception during recursive_reading {type(e).__name__}, close connection')
                log.exception(f'{e}')
                asyncio.ensure_future(self.reconnect())
                break

    async def read_header(self):
        header = await self.stream.read_bytes(FULL_HEADER_LENGTH)
        self._header = header[:HEADER_LENGTH]

        body_length = int32_unpack(header[-4:])
        return body_length

    async def read_body(self, body_length):
        body = await self.stream.read_bytes(body_length)
        version, flags, stream_id, opcode = v3_header_unpack(self._header)

        if not self.request_ids and self.highest_request_id >= self.max_request_id:
            self.status_callback(self.identifier, True)

        self.request_ids.append(stream_id)

        start_time = time.time()
        response = ProtocolHandler.decode_message(
            DEFAULT_CQL_VERSION_NUMBER, None, stream_id, flags, opcode, body, None, None)
        log.debug('decode response time: %.2f ms', (time.time() - start_time) * 1000)
        self._callbacks[stream_id](response)

    def _handle_startup_response(self, message):
        if isinstance(message, ReadyMessage):
            log.debug('got ReadyMessage on connection from %s', self.host)
            self.reconnect_timeout = RECONNECT_TIMEOUT
            self.status_callback(self.identifier, True)
            log.info('connection to %s established', self.host)
        elif isinstance(message, ErrorMessage):
            log.info('closing connection to %s due to startup error: %s', self.host, message.summary_msg())
            raise StartupException2()
        elif isinstance(message, ConnectionShutdownException):
            log.debug('connection to %s was closed during the startup handshake', self.host)
        else:
            log.error('closing connection to %s due to unexpected response during startup: %r', self.host, message)
            raise StartupException3()

    def _connection_prepare(self):
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

        self.stream = IOStream(sock)

    async def send_msg(self, query, callback):
        if self.stream.closed():
            callback(ConnectionShutdownException(self.host))
            return

        request_id = self.get_request_id()

        if request_id >= self.max_request_id:
            self.status_callback(self.identifier, False)

        self._callbacks[request_id] = callback
        message = ProtocolHandler.encode_message(query, request_id, DEFAULT_CQL_VERSION_NUMBER, None, False)
        await self.stream.write(message)

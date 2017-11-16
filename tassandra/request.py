# coding=utf-8


class Request(object):
    def __init__(self, query, future, timeouts, tries=0):
        self._timeout_handler = None

        self.query = query
        self.timeouts = timeouts
        self.future = future

        self.current_connection = None
        self.used_connections_bitmap = 0
        self.tries = tries
        self.failed = False

    def is_retry_possible(self):
        return self.tries < len(self.timeouts)

    def add_timeout(self, io_loop, timeout_callback):
        self._timeout_handler = io_loop.add_timeout(io_loop.time() + self.timeouts[self.tries],
                                                    timeout_callback)

    def register_response(self, io_loop, response):
        if self._timeout_handler is not None:
            io_loop.remove_timeout(self._timeout_handler)
            self._timeout_handler = None
        self.tries += 1
        self.failed = isinstance(response, Exception)

    def send(self, connection, result_callback):
        self.used_connections_bitmap |= connection.identifier
        self.current_connection = connection
        connection.send_msg(self.query, result_callback)

    def __str__(self):
        return 'Request, {} of {} retries'.format(self.tries, len(self.timeouts))

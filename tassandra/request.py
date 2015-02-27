# coding=utf-8


class Request(object):
    def __init__(self, query, future, timeout, retry=0):
        self.query = query
        self.future = future
        self.timeout = timeout
        self.retry = retry
        self.tried = 0
        self.current_connection = None
        self.timeout_handler = None

    def check_retry(self):
        self.retry += 1
        return self.retry < len(self.timeout)

    def get_current_timeout(self):
        return self.timeout[self.retry]

    def __str__(self):
        return 'Request, {} of {} retries'.format(bin(self.tried).count('1'), len(self.timeout))

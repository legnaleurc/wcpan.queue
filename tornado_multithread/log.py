import logging


class Logger(object):

    def __init__(self, name, level):
        super().__init__()
        self._logger = logging.getLogger(name)
        self._level = level
        self._parts = []

    def __lshift__(self, part):
        self._parts.append(str(part))
        return self

    def __del__(self):
        msg = ' '.join(self._parts)
        log = getattr(self._logger, self._level)
        log(msg)


def DEBUG(name):
    return Logger(name, 'debug')


def INFO(name):
    return Logger(name, 'info')


def WARNING(name):
    return Logger(name, 'warning')


def ERROR(name):
    return Logger(name, 'error')


def CRITICAL(name):
    return Logger(name, 'critical')


def EXCEPTION(name):
    return Logger(name, 'exception')

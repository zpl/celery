from __future__ import absolute_import

############## py3k #########################################################
import sys

try:
    from UserList import UserList       # noqa
except ImportError:
    from collections import UserList    # noqa

try:
    from UserDict import UserDict       # noqa
except ImportError:
    from collections import UserDict    # noqa

if sys.version_info >= (3, 0):
    from io import StringIO, BytesIO
    from kombu.utils.encoding import bytes_to_str

    class WhateverIO(StringIO):

        def write(self, data):
            StringIO.write(self, bytes_to_str(data))
else:
    try:
        from cStringIO import StringIO  # noqa
    except ImportError:
        from StringIO import StringIO   # noqa
    BytesIO = WhateverIO = StringIO     # noqa

############## collections.OrderedDict ######################################
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa

############## logging.LoggerAdapter ########################################
import logging
try:
    import multiprocessing
except ImportError:
    multiprocessing = None  # noqa
import sys


def _checkLevel(level):
    if isinstance(level, int):
        rv = level
    elif str(level) == level:
        if level not in logging._levelNames:
            raise ValueError("Unknown level: %r" % level)
        rv = logging._levelNames[level]
    else:
        raise TypeError("Level not an integer or a valid string: %r" % level)
    return rv


class _CompatLoggerAdapter(object):

    def __init__(self, logger, extra):
        self.logger = logger
        self.extra = extra

    def setLevel(self, level):
        self.logger.level = _checkLevel(level)

    def process(self, msg, kwargs):
        kwargs["extra"] = self.extra
        return msg, kwargs

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)
    warn = warning

    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs.setdefault("exc_info", 1)
        self.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.log(logging.CRITICAL, msg, *args, **kwargs)
    fatal = critical

    def log(self, level, msg, *args, **kwargs):
        if self.logger.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self._log(level, msg, args, **kwargs)

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
            func=None, extra=None):
        rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func)
        if extra is not None:
            for key, value in extra.items():
                if key in ("message", "asctime") or key in rv.__dict__:
                    raise KeyError(
                            "Attempt to override %r in LogRecord" % key)
                rv.__dict__[key] = value
        if multiprocessing is not None:
            rv.processName = multiprocessing.current_process()._name
        else:
            rv.processName = ""
        return rv

    def _log(self, level, msg, args, exc_info=None, extra=None):
        defcaller = "(unknown file)", 0, "(unknown function)"
        if logging._srcfile:
            # IronPython doesn't track Python frames, so findCaller
            # throws an exception on some versions of IronPython.
            # We trap it here so that IronPython can use logging.
            try:
                fn, lno, func = self.logger.findCaller()
            except ValueError:
                fn, lno, func = defcaller
        else:
            fn, lno, func = defcaller
        if exc_info:
            if not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
        record = self.makeRecord(self.logger.name, level, fn, lno, msg,
                                 args, exc_info, func, extra)
        self.logger.handle(record)

    def isEnabledFor(self, level):
        return self.logger.isEnabledFor(level)

    def addHandler(self, hdlr):
        self.logger.addHandler(hdlr)

    def removeHandler(self, hdlr):
        self.logger.removeHandler(hdlr)

    @property
    def level(self):
        return self.logger.level


try:
    from logging import LoggerAdapter
except ImportError:
    LoggerAdapter = _CompatLoggerAdapter  # noqa

############## itertools.izip_longest #######################################

try:
    from itertools import izip_longest
except ImportError:
    import itertools

    def izip_longest(*args, **kwds):  # noqa
        fillvalue = kwds.get("fillvalue")

        def sentinel(counter=([fillvalue] * (len(args) - 1)).pop):
            yield counter()     # yields the fillvalue, or raises IndexError

        fillers = itertools.repeat(fillvalue)
        iters = [itertools.chain(it, sentinel(), fillers)
                    for it in args]
        try:
            for tup in itertools.izip(*iters):
                yield tup
        except IndexError:
            pass

############## itertools.chain.from_iterable ################################
from itertools import chain


def _compat_chain_from_iterable(iterables):
    for it in iterables:
        for element in it:
            yield element

try:
    chain_from_iterable = getattr(chain, "from_iterable")
except AttributeError:
    chain_from_iterable = _compat_chain_from_iterable

"""

celery.task.control
===================

The worker remote control command client.
For the server implementation see :mod:`celery.worker.control`.

"""
from __future__ import absolute_import

from .. import current_app
from ..local import Proxy

__all__ = ["broadcast", "rate_limit", "time_limit", "ping", "revoke",
           "purge", "inspect"]

broadcast = Proxy(lambda: current_app.control.broadcast)
rate_limit = Proxy(lambda: current_app.control.rate_limit)
time_limit = Proxy(lambda: current_app.control.time_limit)
ping = Proxy(lambda: current_app.control.ping)
revoke = Proxy(lambda: current_app.control.revoke)
purge = Proxy(lambda: current_app.control.purge)
inspect = Proxy(lambda: current_app.control.inspect)

discard_all = purge  # XXX deprecate

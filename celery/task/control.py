# -*- coding: utf-8 -*-
"""
    celery.task.control
    ~~~~~~~~~~~~~~~~~~~

    Client for worker remote control commands.
    See module :mod:`celery.app.control`.
    Server implementation is in :mod:`celery.worker.control`.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..local import Proxy

broadcast = Proxy(lambda: current_app.control.broadcast)
rate_limit = Proxy(lambda: current_app.control.rate_limit)
time_limit = Proxy(lambda: current_app.control.time_limit)
ping = Proxy(lambda: current_app.control.ping)
revoke = Proxy(lambda: current_app.control.revoke)
purge = Proxy(lambda: current_app.control.purge)
inspect = Proxy(lambda: current_app.control.inspect)

discard_all = purge  # XXX deprecate

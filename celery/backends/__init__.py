# -*- coding: utf-8 -*-
from __future__ import absolute_import

import sys

from .. import current_app
from ..local import Proxy
from ..utils.functional import memoize
from ..utils.imports import get_cls_by_name

UNKNOWN_BACKEND = """\
Unknown result backend: %r.  Did you spell that correctly? (%r)\
"""

BACKEND_ALIASES = {
    "amqp": "celery.backends.amqp:AMQPBackend",
    "cache": "celery.backends.cache:CacheBackend",
    "redis": "celery.backends.redis:RedisBackend",
    "mongodb": "celery.backends.mongodb:MongoBackend",
    "tyrant": "celery.backends.tyrant:TyrantBackend",
    "database": "celery.backends.database:DatabaseBackend",
    "cassandra": "celery.backends.cassandra:CassandraBackend",
    "disabled": "celery.backends.base:DisabledBackend",
}


@memoize(100)
def get_backend_cls(backend=None, loader=None):
    """Get backend class by name/alias"""
    backend = backend or "disabled"
    loader = loader or current_app.loader
    aliases = dict(BACKEND_ALIASES, **loader.override_backends)
    try:
        return get_cls_by_name(backend, aliases)
    except ValueError, exc:
        raise ValueError, ValueError(UNKNOWN_BACKEND % (
                    backend, exc)), sys.exc_info()[2]


# TODO: deprecate this.
# but seems to be used in the wild (e.g. by Zamboni:
# http://nullege.com/codes/search/celery.backends.default_backend)
default_backend = Proxy(lambda: current_app.backend)

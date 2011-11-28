# -*- coding: utf-8 -*-
"""
    celery.app.base
    ~~~~~~~~~~~~~~~

    Application Base Class.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import os

from contextlib import contextmanager
from copy import deepcopy

from kombu.clocks import LamportClock

from .. import datastructures
from .. import platforms
from ..utils import cached_property, lpmerge
from ..utils.imports import instantiate, get_cls_by_name

from .defaults import DEFAULTS, find_deprecated_settings, NAMESPACES

import kombu
if kombu.VERSION < (1, 3, 1):
    raise ImportError("Celery requires Kombu version 1.3.1 or higher.")

BUGREPORT_INFO = """
platform -> system:%(system)s arch:%(arch)s imp:%(py_i)s
software -> celery:%(celery_v)s kombu:%(kombu_v)s py:%(py_v)s
settings -> transport:%(transport)s results:%(results)s
"""


def _url_or_map(params):
    """Converts URL values in the :setting:`BROKERS` setting to dicts."""
    if isinstance(params, basestring):
        return {"hostname": params}
    return params


class Settings(datastructures.ConfigurationView):

    @property
    def BROKERS(self):
        brokers = self.get("BROKERS") or {}
        defaults = dict((opt.key, opt.default)
                    for opt in NAMESPACES["BROKER"].itervalues() if opt.key)
        brokers.setdefault(self.BROKER_DEFAULT, {"hostname": self.BROKER_HOST})
        return dict((name, lpmerge(defaults, _url_or_map(params)))
                        for name, params in brokers.iteritems())

    @property
    def BROKER_BACKEND(self):
        """Deprecated compat alias to :attr:`BROKER_TRANSPORT`."""
        return self.BROKER_TRANSPORT

    @property
    def BROKER_DEFAULT(self):
        return (os.environ.get("CELERY_BROKER_DEFAULT") or
                self.get("BROKER_DEFAULT"))

    @property
    def BROKER_HOST(self):
        return (os.environ.get("CELERY_BROKER_URL") or
                self.get("BROKER_URL") or
                self.get("BROKER_HOST"))

    @property
    def BROKER_TRANSPORT(self):
        """Resolves compat aliases :setting:`BROKER_BACKEND`
        and :setting:`CARROT_BACKEND`."""
        return (self.get("BROKER_TRANSPORT") or
                self.get("BROKER_BACKEND") or
                self.get("CARROT_BACKEND"))

    @property
    def CELERY_RESULT_BACKEND(self):
        """Resolves deprecated alias ``CELERY_BACKEND``."""
        return self.get("CELERY_RESULT_BACKEND") or self.get("CELERY_BACKEND")


class BaseApp(object):
    """Base class for apps."""
    SYSTEM = platforms.SYSTEM
    IS_OSX = platforms.IS_OSX
    IS_WINDOWS = platforms.IS_WINDOWS

    amqp_cls = "celery.app.amqp.AMQP"
    backend_cls = None
    events_cls = "celery.app.events.Events"
    loader_cls = "celery.loaders.app.AppLoader"
    log_cls = "celery.app.log.Logging"
    control_cls = "celery.app.control.Control"
    registry_cls = "celery.app.registry.tasks"

    def __init__(self, main=None, loader=None, backend=None,
            amqp=None, events=None, log=None, control=None,
            set_as_current=True, tasks=None, **kwargs):
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.backend_cls = backend or self.backend_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self.loader_cls
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.set_as_current = set_as_current
        self.clock = LamportClock()
        self._tasks = get_cls_by_name(self.registry_cls)
        self._tasks.update(tasks or {})
        self.on_init()

    def on_init(self):
        """Called at the end of the constructor."""
        pass

    def config_from_object(self, obj, silent=False):
        """Read configuration from object, where object is either
        a object, or the name of a module to import.

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

        """
        del(self.conf)
        return self.loader.config_from_object(obj, silent=silent)

    def config_from_envvar(self, variable_name, silent=False):
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

        """
        del(self.conf)
        return self.loader.config_from_envvar(variable_name, silent=silent)

    def config_from_cmdline(self, argv, namespace="celery"):
        """Read configuration from argv.

        The config

        """
        self.conf.update(self.loader.cmdline_config_parser(argv, namespace))

    def send_task(self, name, args=None, kwargs=None, countdown=None,
            eta=None, task_id=None, producer=None, connection=None,
            result_cls=None, expires=None, router=None, **options):
        """Send task by name.

        :param name: Name of task to execute (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Supports the same arguments as
        :meth:`~celery.app.task.BaseTask.apply_async`.

        """
        router = router or self.amqp.router
        result_cls = result_cls or self.AsyncResult

        # XXX to deprecate
        publisher = options.pop("publisher", None)
        producer = producer or publisher

        options = router.route(options, name, args, kwargs)
        options.setdefault("compression",
                           self.conf.CELERY_MESSAGE_COMPRESSION)

        with self.acquire_producer(connection, producer, block=True) as prod:
            return result_cls(prod.send_task(name, args, kwargs,
                                             task_id=task_id,
                                             countdown=countdown, eta=eta,
                                             expires=expires, **options))

    def AsyncResult(self, task_id, backend=None, task_name=None):
        """Create :class:`celery.result.BaseAsyncResult` instance."""
        from ..result import BaseAsyncResult
        return BaseAsyncResult(task_id, app=self, task_name=task_name,
                               backend=backend or self.backend)

    def TaskSetResult(self, taskset_id, results, **kwargs):
        """Create :class:`celery.result.TaskSetResult` instance."""
        from ..result import TaskSetResult
        return TaskSetResult(taskset_id, results, app=self)

    @contextmanager
    def acquire_producer(self, connection, producer=None, **kwargs):
        if producer:
            yield producer
        else:
            connection = self.broker_connection(connection)
            with self.amqp.producers[connection].acquire(**kwargs) as pub:
                yield pub

    def acquire_connection(self, connection=None, **kwargs):
        return self.amqp.connections[self.broker_connection(connection)] \
                                    .acquire(**kwargs)

    def broker_connection(self, url=None, *args, **kwargs):
        """Establish a connection to the message broker.

        Default values are taken from the default broker (found in
        :setting:`BROKERS`).

        :param URL_or_broker_alias:  This can be either a broker alias
          defined in the :setting:`BROKERS` setting, or a kombu AMQP URL.
          If not provided then the default connection will be used.

        :keyword hostname: or the ``hostname`` field of the default broker.
        :keyword userid: or the ``userid`` field of the default broker.
        :keyword password: or the ``password`` field of the default broker.
        :keyword virtual_host: or the ``virtual_host`` of the default broker.
        :keyword port: or the ``port`` field of the default broker.
        :keyword ssl: or the ``ssl`` field of the default broker.
        :keyword connect_timeout: or the ``connect_timeout`` field of the
            default broker.
        :keyword transport: or the ``transport`` field of the default broker.
        :keyword transport_options: Additional transport-specific options.

        :returns :class:`kombu.connection.BrokerConnection`:

        """
        # we handle several cases here for backward compatibility:
        #
        #   1) broker_connection("alias")
        #
        #        Use default connection parameters from conf.BROKERS["alias"]
        #
        #   2) broker_connection(None)
        #
        #        Some other function supporting a connection argument
        #        called this and the user provided None.  This means
        #        broker_connection can be used as a type of sorts, coercing
        #        several different types of invocation into a working
        #        connection instance.
        #
        #   3) broker_connection(BrokerConnection("redis://"))
        #
        #        Same as 2.
        #
        #   4) broker_connection("localhost")
        #
        #        Don't think this is wildly used, but there is a chance that
        #        it is, so we need to support it, maybe add a deprecation
        #        warning, so only URLs or explicit keyword arguments are
        #        used in the future.
        #
        #   5) broker_connection(hostname="localhost", userid="guest",
        #                        password="guest": port=5672")
        #
        #       Using the same keyword arguments as kombu.BrokerConnection.
        #
        #   5) broker_connection("redis://localhost:312")
        #
        #        Using kombu URLs, this should be allowed.
        #
        args = list(args)
        conf = self.conf
        brokers = conf.BROKERS
        alias = conf.BROKER_DEFAULT
        if url is not None:
            if not isinstance(url, basestring):
                return url  # already connection instance.
            else:
                if url in brokers:
                    alias = url
                else:
                    args.insert(0, url)
        kwargs = dict(brokers[alias], **kwargs)
        if args:
            kwargs.pop("hostname", None)
        return self.amqp.BrokerConnection(*args, **kwargs)

    @contextmanager
    def connection_or_acquire(self, connection=None):
        """For use within a with-statement to get a connection from the pool
        if one is not already provided.

        :keyword connection: If not provided, then a connection will be
                             acquired from the connection pool.
        """
        if connection and connection.connected:
            yield connection
        else:
            with self.acquire_connection(connection, block=True) as conn:
                yield conn
    default_connection = connection_or_acquire

    def prepare_config(self, c):
        """Prepare configuration before it is merged with the defaults."""
        find_deprecated_settings(c)
        return c

    def mail_admins(self, subject, body, fail_silently=False):
        """Send an email to the admins in the :setting:`ADMINS` setting."""
        if self.conf.ADMINS:
            to = [admin_email for _, admin_email in self.conf.ADMINS]
            return self.loader.mail_admins(subject, body, fail_silently, to=to,
                                       sender=self.conf.SERVER_EMAIL,
                                       host=self.conf.EMAIL_HOST,
                                       port=self.conf.EMAIL_PORT,
                                       user=self.conf.EMAIL_HOST_USER,
                                       password=self.conf.EMAIL_HOST_PASSWORD,
                                       timeout=self.conf.EMAIL_TIMEOUT,
                                       use_ssl=self.conf.EMAIL_USE_SSL,
                                       use_tls=self.conf.EMAIL_USE_TLS)

    def merge(self, l, r):
        """Like `dict(a, **b)` except it will keep values from `a`
        if the value in `b` is :const:`None`."""
        return lpmerge(l, r)

    def _get_backend(self):
        from ..backends import get_backend_cls
        backend_cls = self.backend_cls or self.conf.CELERY_RESULT_BACKEND
        backend_cls = get_backend_cls(backend_cls, loader=self.loader)
        return backend_cls(app=self)

    def _get_config(self):
        return Settings({}, [self.prepare_config(self.loader.conf),
                             deepcopy(DEFAULTS)])

    def bugreport(self):
        import celery
        import kombu
        return BUGREPORT_INFO % {"system": self.SYSTEM,
                                 "arch": platforms.architecture(),
                                 "py_i": platforms.pyimplementation(),
                                 "celery_v": celery.__version__,
                                 "kombu_v": kombu.__version__,
                                 "py_v": platforms.python_version(),
                                 "transport": self.conf.BROKER_TRANSPORT,
                                 "results": self.conf.CELERY_RESULT_BACKEND}

    @property
    def pool(self):
        return self.amqp.connections[self.broker_connection()]

    @cached_property
    def amqp(self):
        """Sending/receiving messages.  See :class:`~celery.app.amqp.AMQP`."""
        return instantiate(self.amqp_cls, app=self)

    @cached_property
    def backend(self):
        """Storing/retrieving task state.  See
        :class:`~celery.backend.base.BaseBackend`."""
        return self._get_backend()

    @cached_property
    def conf(self):
        """Current configuration (dict and attribute access)."""
        return self._get_config()

    @cached_property
    def control(self):
        """Controlling worker nodes.  See
        :class:`~celery.app.control.Control`."""
        return instantiate(self.control_cls, app=self)

    @cached_property
    def events(self):
        """Sending/receiving events.  See :class:`~celery.events.Events`. """
        return instantiate(self.events_cls, app=self)

    @cached_property
    def loader(self):
        """Current loader."""
        from ..loaders import get_loader_cls
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self):
        """Logging utilities.  See :class:`~celery.log.Logging`."""
        return instantiate(self.log_cls, app=self)

    @cached_property
    def tasks(self):
        from .task import builtins
        builtins.load_builtins(self)
        return self._tasks

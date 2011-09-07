# -*- coding: utf-8 -*-
"""
celery.app.amqp
===============

AMQ related functionality.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from collections import defaultdict
from datetime import datetime, timedelta

from kombu import BrokerConnection, Consumer, Exchange, Producer, Queue
from kombu import pools
from kombu.common import entry_to_queue

from .. import signals
from ..utils import cached_property, textindent, uuid

from . import routes as _routes

# UTC timezone mark.
TZ_UTC = 0x1

#: List of known options to a Kombu producers send method.
#: Used to extract the message related options out of any `dict`.
MSG_OPTIONS = ("mandatory", "priority", "immediate", "routing_key",
                "serializer", "delivery_mode", "compression")

#: Human readable queue declaration.
QUEUE_FORMAT = """
. %(name)s exchange:%(exchange)s (%(exchange_type)s) \
binding:%(routing_key)s
"""

#: Set of exchange names that have already been declared.
_exchanges_declared = defaultdict(lambda: set())

#: Set of queue names that have already been declared.
_queues_declared = defaultdict(lambda: set())


def extract_msg_options(options, keep=MSG_OPTIONS):
    """Extracts known options to `basic_publish` from a dict,
    and returns a new dict."""
    return dict((name, options.get(name)) for name in keep)


class Queues(dict):
    """Queue name⇒ declaration mapping.

    Celery will consult this mapping to find the options
    for any queue by name.

    :param queues: Initial mapping.

    """
    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(self, queues):
        dict.__init__(self)
        for queue in (queues or ()):
            self.add(queue)

    def add(self, queue):
        """Add new queue.

        :param queue: Name of the queue.
        :keyword exchange: Name of the exchange.
        :keyword routing_key: Binding key.
        :keyword exchange_type: Type of exchange.
        :keyword \*\*options: Additional declaration options.

        """
        self[queue.name] = queue
        return queue

    def add_missing(self, name):
        q = self[name] = self.new_missing(name)
        return q

    def new_missing(self, name):
        return Queue(name, Exchange(name, "direct"), name)

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ""

        def as_info(q):
            return {"name": (q.name + ":").ljust(12),
                    "exchange": q.exchange.name,
                    "exchange_type": q.exchange.type,
                    "routing_key": q.routing_key}

        info = [QUEUE_FORMAT.strip() % as_info(queue)
                        for _, queue in sorted(active.iteritems())]
        if indent_first:
            return textindent("\n".join(info), indent)
        return info[0] + "\n" + textindent("\n".join(info[1:]), indent)

    def select_subset(self, wanted, create_missing=True):
        """Select subset of the currently defined queues.

        Does not return anything: queues not in `wanted` will
        be discarded in-place.

        :param wanted: List of wanted queue names.
        :keyword create_missing: By default any unknown queues will be
                                 added automatically, but if disabled
                                 the occurrence of unknown queues
                                 in `wanted` will raise :exc:`KeyError`.

        """
        acc = {}
        for queue in wanted:
            try:
                q = self[queue]
            except KeyError:
                if not create_missing:
                    raise
                q = self.new_missing(queue)
            acc[queue] = q
        self._consume_from = acc
        self.update(acc)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self

    @classmethod
    def with_defaults(cls, queues, default_exchange):
        """Alternate constructor that adds default exchange and
        exchange type information to queues that does not have any."""
        if queues is None:
            queues = ()
        for queue in queues:
            if queue.exchange is None or not queue.exchange.name:
                queue.exchange = default_exchange
            if queue.routing_key is None:
                queue.routing_key = default_exchange.name
        return cls(queues)


def maybe_exchange(exchange, exchange_type):
    if isinstance(exchange, basestring):
        return Exchange(exchange, type=exchange_type)
    return exchange


class TaskProducer(Producer):
    auto_declare = True
    retry = False
    retry_policy = None

    def __init__(self, connection, *args, **kwargs):
        self.connection = connection
        self.app = kwargs.pop("app")
        self.retry = kwargs.pop("retry", self.retry)
        self.retry_policy = kwargs.pop("retry_policy",
                                        self.retry_policy or {})
        kwargs["exchange"] = maybe_exchange(kwargs.get("exchange"),
                                            kwargs.get("exchange_type"))
        super(TaskProducer, self).__init__(connection.default_channel,
                                           *args, **kwargs)

    def declare(self):
        edeclared = _exchanges_declared[self.connection]
        if self.exchange.name and self.exchange.name not in edeclared:
            super(TaskProducer, self).declare()
            edeclared.add(self.exchange.name)

    def _declare_queue(self, name, retry=False, retry_policy={}):
        queue = self.app.queues[name](self.channel)
        if retry:
            self.connection.ensure(queue, queue.declare, **retry_policy)()
        else:
            queue.declare()
        return queue

    def _declare_exchange(self, exchange, retry=False, retry_policy={}):
        ex = exchange(self.channel)
        if retry:
            return self.connection.ensure(ex, ex.declare, **retry_policy)
        return ex.declare()

    def maybe_declare(self, queue, exchange, retry, retry_policy):
        connection = self.connection
        qdeclared = _queues_declared[connection]
        edeclared = _exchanges_declared[connection]
        if queue and queue not in qdeclared:
            entity = self._declare_queue(queue, retry, retry_policy)
            edeclared.add(entity.exchange.name)
            qdeclared.add(entity.name)
        if exchange and exchange.name not in edeclared:
            self._declare_exchange(exchange, retry, retry_policy)
            edeclared.add(exchange.name)

    def _send_task(self, body, exchange=None, exchange_type=None,
            retry=None, retry_policy=None, queue=None, **options):
        publish = self.publish
        connection = self.connection

        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policy
            _retry_policy = dict(_retry_policy, **retry_policy)

        exchange = maybe_exchange(exchange, exchange_type)
        self.maybe_declare(queue, exchange, retry, _retry_policy)

        if (retry if retry is not None else self.retry):
            publish = connection.ensure(self, self.publish, **_retry_policy)
        publish(body, exchange=exchange, **extract_msg_options(options))


    def send_task(self, name, args=None, kwargs=None,
            countdown=None, eta=None, id=None, taskset_id=None,
            expires=None, event_dispatcher=None, now=None,
            retries=0, chord=None, **options):
        """Send task message."""

        id = id or uuid()
        args = args or []
        kwargs = kwargs or {}
        if not isinstance(args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")
        if countdown:                           # Convert countdown to ETA.
            now = now or datetime.utcnow()
            eta = now + timedelta(seconds=countdown)
        if isinstance(expires, int):
            now = now or datetime.utcnow()
            expires = now + timedelta(seconds=expires)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        body = {"task": name,
                "id": id,
                "args": args or [],
                "kwargs": kwargs or {},
                "retries": retries or 0,
                "eta": eta,
                "expires": expires,
                "tz": TZ_UTC,
                "taskset": taskset_id,
                "chord": chord}

        self._send_task(body, **options)

        signals.task_sent.send(sender=name, **body)
        if event_dispatcher:
            event_dispatcher.send("task-sent",
                            uuid=id, name=name,
                            args=repr(args), kwargs=repr(kwargs),
                            retries=retries, eta=eta, expires=expires)
        return id


class ProducerPool(pools.ProducerPool):

    def __init__(self, type, connections, limit):
        self.type = type
        super(ProducerPool, self).__init__(connections, limit=limit)

    def create_producer(self):
        conn = self.connections.acquire(block=True)
        producer = self.type(conn, auto_declare=False)
        producer.connection = conn
        return producer


class _Producers(pools.PoolGroup):

    def __init__(self, type, connections):
        self.type = type
        self.connections = connections

    def create(self, connection, limit):
        return ProducerPool(self.type,
                            self.connections[connection], limit=limit)


class AMQP(object):
    Consumer = Consumer
    BrokerConnection = BrokerConnection
    Producer = Producer

    #: Cached and prepared routing table.
    _rtable = None

    def __init__(self, app):
        self.app = app

    def Queues(self, queues):
        """Create new :class:`Queues` instance, using queue defaults
        from the current configuration."""
        #: queues can also be dict.
        if not isinstance(queues, Queues) and isinstance(queues, dict):
            queues = [entry_to_queue(name, **opts)
                        for name, opts in queues.iteritems()]
        conf = self.app.conf
        default = conf.CELERY_DEFAULT_QUEUE
        defex = conf.CELERY_DEFAULT_EXCHANGE
        if not isinstance(defex, Exchange):
            defex = Exchange(conf.CELERY_DEFAULT_EXCHANGE,
                             type=conf.CELERY_DEFAULT_EXCHANGE_TYPE)
        if not queues and default:
            if not isinstance(default, Queue):
                default = Queue(default, defex,
                                conf.CELERY_DEFAULT_ROUTING_KEY)
            queues = (default, )
        return Queues.with_defaults(queues, defex)

    def Router(self, queues=None, create_missing=None):
        """Creates new task router."""
        create_missing = (create_missing if create_missing is not None
                            else self.app.conf.CELERY_CREATE_MISSING_QUEUES)
        return _routes.Router(self.routes, queues or self.queues,
                              create_missing, app=self.app)

    def TaskProducer(self, *args, **kwargs):
        """Returns producer used to send tasks.

        You probably want `app.send_task` instead.

        """
        conf = self.app.conf
        defaults = {"serializer": conf.CELERY_TASK_SERIALIZER,
                    "retry": conf.CELERY_TASK_PUBLISH_RETRY,
                    "retry_policy": conf.CELERY_TASK_PUBLISH_RETRY_POLICY,
                    "app": self}
        return TaskProducer(*args, **self.app.merge(defaults, kwargs))

    def get_task_consumer(self, connection, queues=None, **kwargs):
        """Return consumer configured to consume from all active queues."""
        return self.Consumer(connection.channel(),
                             queues=self.queues.consume_from.values(),
                             **kwargs)

    def get_default_queue(self):
        """Returns `(queue_name, queue_options)` tuple for the queue
        configured to be default (:setting:`CELERY_DEFAULT_QUEUE`)."""
        q = self.app.conf.CELERY_DEFAULT_QUEUE
        return q, self.queues[q]

    def flush_routes(self):
        """Reset routing table.

        This needs to be called if changes to :setting:`CELERY_ROUTES`
        are made.

        """
        self._rtable = _routes.prepare(self.app.conf.CELERY_ROUTES)

    @cached_property
    def queues(self):
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.CELERY_QUEUES)

    @property
    def routes(self):
        if self._rtable is None:
            self.flush_routes()
        return self._rtable

    @cached_property
    def connections(self):
        # XXX limit can't be set like this.
        # e.g. creating a new app with a different limit can't destroy
        # the pools of others....
        pools.set_limit(self.app.conf.BROKER_POOL_LIMIT)
        return pools.connections

    @cached_property
    def producers(self):
        return pools.register_group(_Producers(self.TaskProducer,
                                               self.connections))

    @cached_property
    def router(self):
        return self.Router()

# -*- coding: utf-8 -*-
"""
celery.app.amqp
===============

AMQ related functionality.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from datetime import datetime, timedelta

from kombu import BrokerConnection, Exchange, Consumer, Producer
from kombu import pools

from .. import routes as _routes
from .. import signals
from ..utils import cached_property, textindent, uuid

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
_exchanges_declared = set()

#: Set of queue names that have already been declared.
_queues_declared = set()


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
                queue = self[queue]
            except KeyError:
                if not create_missing:
                    raise
                queue = self.new_missing(queue)
            acc[queue] = queue
        self._consume_from = queue
        self.update(queue)

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
        exchange = kwargs.get("exchange")
        if isinstance(exchange, basestring):
            kwargs["exchange"] = Exchange(exchange, "direct")
        super(TaskProducer, self).__init__(connection.default_channel,
                                           *args, **kwargs)

    def declare(self):
        if self.exchange.name and \
                self.exchange.name not in _exchanges_declared:
            super(TaskProducer, self).declare()
            _exchanges_declared.add(self.exchange.name)

    def _declare_queue(self, name, retry=False, retry_policy={}):
        queue = self.app.queues[name](self.channel)
        if retry:
            self.connection.ensure(queue, queue.declare, **retry_policy)()
        else:
            queue.declare()
        return queue

    def _declare_exchange(self, name, type, retry=False, retry_policy={}):
        if isinstance(name, Exchange):
            ex = name(self.channel)
        else:
            ex = Exchange(name, type=type)(self.channel)
        if retry:
            return self.connection.ensure(ex, ex.declare, **retry_policy)
        return ex.declare()

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None,
            expires=None, exchange=None, exchange_type=None,
            event_dispatcher=None, retry=None, retry_policy=None,
            queue=None, now=None, retries=0, chord=None, **kwargs):
        """Send task message."""

        connection = self.connection
        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policy
            _retry_policy = dict(_retry_policy, **retry_policy)

        # declare entities
        if queue and queue not in _queues_declared:
            entity = self._declare_queue(queue, retry, _retry_policy)
            _exchanges_declared.add(entity.exchange.name)
            _queues_declared.add(entity.name)
        if exchange and exchange not in _exchanges_declared:
            self._declare_exchange(exchange,
                    exchange_type or self.exchange.type, retry, _retry_policy)
            _exchanges_declared.add(exchange)

        task_id = task_id or uuid()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")
        if countdown:                           # Convert countdown to ETA.
            now = now or datetime.now()
            eta = now + timedelta(seconds=countdown)
        if isinstance(expires, int):
            now = now or datetime.now()
            expires = now + timedelta(seconds=expires)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        body = {"task": task_name,
                "id": task_id,
                "args": task_args or [],
                "kwargs": task_kwargs or {},
                "retries": retries or 0,
                "eta": eta,
                "expires": expires}

        if taskset_id:
            body["taskset"] = taskset_id
        if chord:
            body["chord"] = chord

        do_retry = retry if retry is not None else self.retry
        publish = self.publish
        if do_retry:
            publish = connection.ensure(self, self.publish, **_retry_policy)
        publish(body, exchange=exchange, **extract_msg_options(kwargs))
        signals.task_sent.send(sender=task_name, **body)
        if event_dispatcher:
            event_dispatcher.send("task-sent", uuid=task_id,
                                               name=task_name,
                                               args=repr(task_args),
                                               kwargs=repr(task_kwargs),
                                               retries=retries,
                                               eta=eta,
                                               expires=expires)
        return task_id

    def __exit__(self, *exc_info):
        try:
            self.release()
        except AttributeError:
            self.close()


class ProducerPool(pools.ProducerPool):

    def __init__(self, app):
        self.app = app
        super(ProducerPool, self).__init__(self.app.pool,
                                           limit=self.app.pool.limit)

    def create_producer(self):
        conn = self.connections.acquire(block=True)
        producer = self.app.amqp.TaskProducer(conn, auto_declare=False)
        producer.connection = conn
        return producer


class AMQP(object):
    Consumer = Consumer
    BrokerConnection = BrokerConnection
    Producer = Producer

    #: Cached and prepared routing table.
    _rtable = None

    def __init__(self, app):
        self.app = app

    def flush_routes(self):
        self._rtable = _routes.prepare(self.app.conf.CELERY_ROUTES)

    def Queues(self, queues):
        """Create new :class:`Queues` instance, using queue defaults
        from the current configuration."""
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
        """Returns the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either("CELERY_CREATE_MISSING_QUEUES",
                                              create_missing), app=self.app)

    def TaskConsumer(self, *args, **kwargs):
        """Returns consumer for a single task queue."""
        _, default_queue = self.get_default_queue()
        return self.Consumer(*args, queues=[default_queue],
                             **self.app.merge(defaults, kwargs))

    def TaskProducer(self, *args, **kwargs):
        """Returns producer used to send tasks.

        You should use `app.send_task` instead.

        """
        conf = self.app.conf
        defaults = {"serializer": conf.CELERY_TASK_SERIALIZER,
                    "retry": conf.CELERY_TASK_PUBLISH_RETRY,
                    "retry_policy": conf.CELERY_TASK_PUBLISH_RETRY_POLICY,
                    "app": self}
        return TaskProducer(*args, **self.app.merge(defaults, kwargs))

    def get_task_consumer(self, connection, queues=None, **kwargs):
        """Return consumer configured to consume from all known task
        queues."""
        print("QUEUES: %r" % (self.queues.consume_from.values(), ))
        return self.Consumer(connection.channel(),
                             queues=self.queues.consume_from.values(),
                             **kwargs)

    def get_default_queue(self):
        """Returns `(queue_name, queue_options)` tuple for the queue
        configured to be default (:setting:`CELERY_DEFAULT_QUEUE`)."""
        q = self.app.conf.CELERY_DEFAULT_QUEUE
        return q, self.queues[q]

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
    def producer_pool(self):
        return ProducerPool(self.app)

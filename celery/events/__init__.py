from __future__ import absolute_import
from __future__ import with_statement

import time
import socket
import threading

from collections import deque

from kombu import Exchange, Producer, Queue
from kombu.mixins import ConsumerMixin
from kombu.log import LogMixin

from ..app import app_or_default
from ..app.amqp import merge_policy
from ..utils import uuid

event_exchange = Exchange("celeryev", type="topic")


def Event(type, _fields=None, **fields):
    """Create an event.

    An event is a dictionary, the only required field is ``type``.

    """
    event = dict(_fields or {}, type=type, **fields)
    if "timestamp" not in event:
        event["timestamp"] = time.time()
    return event


class EventDispatcher(LogMixin):
    """Send events as messages.

    :param connection: Connection to the broker.

    :keyword hostname: Hostname to identify ourselves as,
        by default uses the hostname returned by :func:`socket.gethostname`.

    :keyword enabled: Set to :const:`False` to not actually publish any events,
        making :meth:`send` a noop operation.

    :keyword channel: Can be used instead of `connection` to specify
        an exact channel to use when sending events.

    :keyword buffer_while_offline: If enabled events will be buffered
       while the connection is down. :meth:`flush` must be called
       as soon as the connection is re-established.

    You need to :meth:`close` this after use.

    """

    def __init__(self, connection=None, hostname=None, enabled=True,
            channel=None, buffer_while_offline=True, app=None,
            serializer=None, retry=False, retry_policy=None):
        self.app = app_or_default(app)
        self.connection = connection
        self.channel = channel
        self.hostname = hostname or socket.gethostname()
        self.buffer_while_offline = buffer_while_offline
        self.mutex = threading.Lock()
        self.producer = None
        self._outbound_buffer = deque()
        self.serializer = serializer or self.app.conf.CELERY_EVENT_SERIALIZER
        self.retry = retry
        self.retry_policy = self.app.amqp.merge_retry_policy(retry_policy)

        self.enabled = enabled
        if self.enabled:
            self.enable()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def enable(self):
        if self.producer is None:
            channel = self.channel or self.connection
            self.producer = Producer(channel, serializer=self.serializer,
                                              auto_declare=False)
        self.enabled = True

    def disable(self):
        if self.enabled:
            self.enabled = False
            self.close()

    def publish(self, type, fields, retry=None, retry_policy=None):
        if not self.enabled:
            return
        retry = retry if retry is not None else self.retry
        retry_policy = merge_policy(retry_policy, self.retry_policy)
        event = Event(type, hostname=self.hostname,
                            clock=self.app.clock.forward(), **fields)
        with self.mutex:
            try:
                self.producer.publish(event,
                                      exchange=event_exchange,
                                      declare=[event_exchange],
                                      routing_key=type.replace("-", "."),
                                      retry=retry,
                                      retry_policy=retry_policy)
            except Exception, exc:
                if not self.buffer_while_offline:
                    raise
                self.error("Event buffered for error: %r", exc)
                self._outbound_buffer.append((type, fields, exc))


    def send(self, type, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword \*\*fields: Event arguments.

        """
        return self.publish(type, fields)


    def flush(self):
        while self._outbound_buffer:
            try:
                type, fields, _ = self._outbound_buffer.popleft()
            except IndexError:
                return
            self.send(type, **fields)

    def copy_buffer(self, other):
        self._outbound_buffer = other._outbound_buffer

    def close(self):
        """Close the event dispatcher."""
        self.mutex.locked() and self.mutex.release()
        if self.producer is not None:
            self.producer = self.producer.release()


class EventReceiver(ConsumerMixin):
    """Capture events.

    :param connection: Connection to the broker.
    :keyword handlers: Event handlers.

    :attr:`handlers` is a dict of event types and their handlers,
    the special handler `"*"` captures all events that doesn't have a
    handler.

    """
    handlers = {}

    def __init__(self, connection, handlers=None, routing_key="#",
            node_id=None, app=None):
        self.app = app_or_default(app)
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue = Queue("%s.%s" % ("celeryev", self.node_id),
                           exchange=event_exchange,
                           routing_key=self.routing_key,
                           auto_delete=True,
                           durable=False)

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        return self.consume(limit=limit, timeout=timeout, wakeup=wakeup)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        return list(self.consume(limit=limit, timeout=timeout, wakeup=wakeup))

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=True)]

    def on_consume_ready(self, connection, channel, consumers,
            wakeup=True, **kwargs):
        if wakeup:
            self.wakeup_workers(channel=channel)

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast("heartbeat",
                                   connection=self.connection,
                                   channel=channel)

    def _receive(self, body, message):
        type = body.pop("type").lower()
        clock = body.get("clock")
        if clock:
            self.app.clock.adjust(clock)
        self.process(type, Event(type, body))

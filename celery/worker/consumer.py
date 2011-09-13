from __future__ import absolute_import
from __future__ import with_statement

"""

This module contains the component responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.


* :meth:`~Consumer.start` is an infinite loop, which only iterates
  again if the connection is lost. For each iteration (at start, or if the
  connection is lost) it calls :meth:`~Consumer.reset_connection`,
  and starts the consumer by calling :meth:`~Consumer.consume_messages`.

* :meth:`~Consumer.reset_connection`, clears the internal queues,
  establishes a new connection to the broker, sets up the task
  consumer (+ QoS), and the broadcast remote control command consumer.

  Also if events are enabled it configures the event dispatcher and starts
  up the heartbeat thread.

* Finally it can consume messages. :meth:`~Consumer.consume_messages`
  is simply an infinite loop waiting for events on the AMQP channels.

  Both the task consumer and the broadcast consumer uses the same
  callback: :meth:`~Consumer.receive_message`.

* So for each message received the :meth:`~Consumer.receive_message`
  method is called.

  If the message is a task, it verifies the validity of the message
  converts it to a :class:`celery.worker.job.TaskRequest`, and sends
  it to :meth:`~Consumer.on_task`.

  It also tries to handle malformed or invalid messages properly,
  so the worker doesn't choke on them and die. Any invalid messages
  are acknowledged immediately and logged, so the message is not resent
  again, and again.

* If the task has an ETA/countdown, the task is moved to the `eta_schedule`
  so the :class:`timer2.Timer` can schedule it at its
  deadline. Tasks without an eta are moved immediately to the `ready_queue`,
  so they can be picked up by the :class:`~celery.worker.mediator.Mediator`
  to be sent to the pool.

* When a task with an ETA is received the QoS prefetch count is also
  incremented, so another message can be reserved. When the ETA is met
  the prefetch count is decremented again, though this cannot happen
  immediately because amqplib doesn't support doing broker requests
  across threads. Instead the current prefetch count is kept as a
  shared counter, so as soon as  :meth:`~Consumer.consume_messages`
  detects that the value has changed it will send out the actual
  QoS event to the broker.

*  If the message is a control command the message is passed to
  :meth:`~Consumer.on_control`, which in turn dispatches
  the control command using the control dispatcher.

* Notice that when the connection is lost all internal queues are cleared
  because we can no longer ack the messages reserved in memory.
  However, this is not dangerous as the broker will resend them
  to another worker when the channel is closed.

* **WARNING**: :meth:`~Consumer.stop` does not close the connection!
  This is because some pre-acked messages may be in processing,
  and they need to be finished before the channel is closed.
  For celeryd this means the pool must finish the tasks it has acked
  early, *then* close the connection.

"""
import socket
import sys
import threading
import traceback
import warnings

from contextlib import contextmanager
from functools import partial

from kombu.mixins import ConsumerMixin

from ..app import app_or_default
from ..datastructures import AttributeDict
from ..exceptions import NotRegistered
from ..utils import timer2
from ..utils.encoding import safe_repr
from ..utils.functional import noop

from . import state
from .job import TaskRequest, InvalidTaskError
from .control.registry import Panel
from .heartbeat import Heart

RUN = 0x1
CLOSE = 0x2

#: Prefetch count can't exceed short.
PREFETCH_COUNT_MAX = 0xFFFF

#: Error message for when an unregistered task is received.
UNKNOWN_TASK_ERROR = """\
Received unregistered task of type %s.
The message has been ignored and discarded.

Did you remember to import the module containing this task?
Or maybe you are using relative imports?
Please see http://bit.ly/gLye1c for more information.

The full contents of the message body was:
%s
"""

#: Error message for when an invalid task message is received.
INVALID_TASK_ERROR = """\
Received invalid task message: %s
The message has been ignored and discarded.

Please ensure your message conforms to the task
message protocol as described here: http://bit.ly/hYj41y

The full contents of the message body was:
%s
"""

MESSAGE_REPORT_FMT = """\
body: %s {content_type:%s content_encoding:%s delivery_info:%s}\
"""


class QoS(object):
    """Quality of Service for Channel.

    For thread-safe increment/decrement of a channels prefetch count value.

    :param consumer: A :class:`kombu.messaging.Consumer` instance.
    :param initial_value: Initial prefetch count value.
    :param logger: Logger used to log debug messages.

    """
    prev = None

    def __init__(self, consumer, initial_value, logger):
        self.consumer = consumer
        self.logger = logger
        self._mutex = threading.RLock()
        self.value = initial_value

    def increment(self, n=1):
        """Increment the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                new_value = self.value + max(n, 0)
                self.value = self.set(new_value)
        return self.value

    def _sub(self, n=1):
        assert self.value - n > 1
        self.value -= n

    def decrement(self, n=1):
        """Decrement the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                self._sub(n)
                self.set(self.value)
        return self.value

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the qos.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        with self._mutex:
            if self.value:
                self._sub(n)

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        if pcount != self.prev:
            new_value = pcount
            if pcount > PREFETCH_COUNT_MAX:
                self.logger.warning("QoS: Disabled: prefetch_count exceeds %r",
                                    PREFETCH_COUNT_MAX)
                new_value = 0
            self.logger.debug("basic.qos: prefetch_count->%s", new_value)
            self.consumer.qos(prefetch_count=new_value)
            self.prev = pcount
        return pcount

    def update(self):
        """Update prefetch count with current value."""
        with self._mutex:
            return self.set(self.value)


class Consumer(ConsumerMixin):
    """Listen for messages received from the broker and
    move them to the ready queue for task processing.

    :param ready_queue: See :attr:`ready_queue`.
    :param eta_schedule: See :attr:`eta_schedule`.

    """

    #: The queue that holds tasks ready for immediate processing.
    ready_queue = None

    #: Timer for tasks with an ETA/countdown.
    eta_schedule = None

    #: Enable/disable events.
    send_events = False

    #: Optional callback to be called when the connection is established.
    #: Will only be called once, even if the connection is lost and
    #: re-established.
    init_callback = None

    #: The current hostname.  Defaults to the system hostname.
    hostname = None

    #: Initial QoS prefetch count for the task channel.
    initial_prefetch_count = 0

    #: A :class:`celery.events.EventDispatcher` for sending events.
    event_dispatcher = None

    #: The thread that sends event heartbeats at regular intervals.
    #: The heartbeats are used by monitors to detect that a worker
    #: went offline/disappeared.
    heart = None

    #: The current worker pool instance.
    pool = None

    #: A timer used for high-priority internal tasks, such
    #: as sending heartbeats.
    priority_timer = None

    # Consumer state, can be RUN or CLOSE.
    _state = None

    # List of active consumer instances.
    _consumers = None

    def __init__(self, ready_queue, eta_schedule, logger,
            init_callback=noop, send_events=False, hostname=None,
            initial_prefetch_count=2, pool=None, app=None,
            priority_timer=None, controller=None, connection=None):
        self.app = app_or_default(app)
        self.connection = self.app.broker_connection(connection)
        self.controller = controller
        self.ready_queue = ready_queue
        self.eta_schedule = eta_schedule
        self.send_events = send_events
        self.init_callback = init_callback
        self.hostname = hostname or socket.gethostname()
        self.initial_prefetch_count = initial_prefetch_count
        self.pool = pool
        self._logger = logger
        self.priority_timer = priority_timer or timer2.default_timer

    def start(self):
        self.run()

    def create_pidbox_node(self, channel):
        return self.app.control.mailbox.Node(self.hostname,
                        channel=channel, handlers=Panel.data,
                        state=AttributeDict(app=self.app,
                                            logger=self.logger,
                                            hostname=self.hostname,
                                            consumer=self))

    def on_iteration(self):
        if self.qos.prev != self.qos.value:
            self.qos.update()

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self.debug("Ready to accept tasks!")
        self.consumers = consumers

    def on_task(self, task):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """

        if task.revoked():
            return

        self.logger.info("Got task from broker: %s", task.shortinfo())

        if self.event_dispatcher.enabled:
            self.event_dispatcher.send("task-received", uuid=task.task_id,
                    name=task.task_name, args=safe_repr(task.args),
                    kwargs=safe_repr(task.kwargs), retries=task.retries,
                    eta=task.eta and task.eta.isoformat(),
                    expires=task.expires and task.expires.isoformat())

        if task.eta:
            try:
                eta = timer2.to_timestamp(task.eta)
            except OverflowError, exc:
                self.logger.error(
                    "Couldn't convert eta %s to timestamp: %r. Task: %r",
                    task.eta, exc, task.info(safe=True),
                    exc_info=sys.exc_info())
                task.acknowledge()
            else:
                self.qos.increment()
                self.eta_schedule.apply_at(eta,
                                           self.apply_eta_task, (task, ))
        else:
            state.task_reserved(task)
            self.ready_queue.put(task)

    def on_control(self, node, body, message):
        """Process remote control command message."""
        try:
            node.handle_message(body, message)
        except KeyError, exc:
            self.error("No such control command: %s", exc)
        except Exception, exc:
            self.error(
                "Error occurred while handling control command: %r\n%r",
                    exc, traceback.format_exc(), exc_info=sys.exc_info())

    def apply_eta_task(self, task):
        """Method called by the timer to apply a task with an
        ETA/countdown."""
        state.task_reserved(task)
        self.ready_queue.put(task)
        self.qos.decrement_eventually()

    def _message_report(self, body, message):
        return MESSAGE_REPORT_FMT % (safe_repr(body),
                                     safe_repr(message.content_type),
                                     safe_repr(message.content_encoding),
                                     safe_repr(message.delivery_info))

    def receive_message(self, body, message):
        """Handles incoming messages.

        :param body: The message body.
        :param message: The kombu message object.

        """
        # need to guard against errors occurring while acking the message.
        def ack():
            try:
                message.ack()
            except self.connection_errors + (AttributeError, ), exc:
                self.logger.critical(
                    "Couldn't ack %r: %s reason:%r",
                        message.delivery_tag,
                        self._message_report(body, message), exc)

        try:
            body["task"]
        except (KeyError, TypeError):
            warnings.warn(RuntimeWarning(
                "Received and deleted unknown message. Wrong destination?!? \
                the full contents of the message body was: %s" % (
                 self._message_report(body, message), )))
            ack()
            return

        try:
            task = TaskRequest.from_message(message, body, ack,
                                            app=self.app,
                                            logger=self.logger,
                                            hostname=self.hostname,
                                            eventer=self.event_dispatcher)

        except NotRegistered, exc:
            self.logger.error(UNKNOWN_TASK_ERROR, exc, safe_repr(body),
                              exc_info=sys.exc_info())
            ack()
        except InvalidTaskError, exc:
            self.logger.error(INVALID_TASK_ERROR, str(exc), safe_repr(body),
                              exc_info=sys.exc_info())
            ack()
        else:
            self.on_task(task)

    def stop_consumers(self, close_connection=True):
        """Stop consuming tasks and broadcast commands, also stops
        the heartbeat thread and event dispatcher.

        :keyword close_connection: Set to False to skip closing the broker
                                    connection.

        """
        if not self._state == RUN:
            return

        if self.heart:
            # Stop the heartbeat thread if it's running.
            self.logger.debug("Heart: Going into cardiac arrest...")
            self.heart = self.heart.stop()

        self.debug("Cancelling consumers...")
        for consumer in (self._consumers or []):
            self.maybe_conn_error(consumer.cancel)

        if self.event_dispatcher:
            self.debug("Shutting down event dispatcher...")
            self.event_dispatcher = \
                    self.maybe_conn_error(self.event_dispatcher.close)

        if close_connection:
            self.should_stop = True

    def get_consumers(self, Consumer, channel):
        tasks = self.app.amqp.get_task_consumer(channel,
                    on_decode_error=self.on_decode_error,
                    callbacks=[self.receive_message])

        # QoS: Reset prefetch window.
        self.qos = QoS(tasks, self.initial_prefetch_count, self.logger)
        self.qos.update()

        return [tasks]

    @contextmanager
    def extra_context(self, connection, channel):
        box = self._pidbox_consumer
        if self.pool is not None and self.pool.is_green:
            box = self._green_pidbox_consumer

        with box(connection, channel):
            yield

    @contextmanager
    def _pidbox_consumer(self, connection, _):
        with connection.channel() as channel:
            node = self.create_pidbox_node(channel)
            callback = partial(self.on_control, node)
            with node.listen(callback=callback):
                yield

    @contextmanager
    def _green_pidbox_consumer(self, connection, _):

        class gPidboxConsumer(ConsumerMixin):

            def __init__(self, c, connection, callback):
                self.c = c
                self.connection = connection
                self.callback = callback

            def get_consumers(self, Consumer, channel):
                node = self.c.create_pidbox_node(channel)
                callback = partial(self.callback, node)
                return [node.listen(callback=callback)]

        gpidbox = gPidboxConsumer(self, connection, self.on_control)
        self.pool.spawn_n(gpidbox.run)
        yield
        gpidbox.should_stop = True

    def on_connection_revived(self):
        self.debug("Connection revived")
        self.init_callback(self)

        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        self.ready_queue.clear()
        self.eta_schedule.clear()

        # Flush events sent while connection was down.
        prev_event_dispatcher = self.event_dispatcher
        self.event_dispatcher = self.app.events.Dispatcher(self.connection,
                                                hostname=self.hostname,
                                                enabled=self.send_events)
        if prev_event_dispatcher:
            self.event_dispatcher.copy_buffer(prev_event_dispatcher)
            self.event_dispatcher.flush()

        # Restart heartbeat thread.
        self.restart_heartbeat()

        # We're back!
        self._state = RUN

    def restart_heartbeat(self):
        """Restart the heartbeat thread.

        This thread sends heartbeat events at intervals so monitors
        can tell if the worker is off-line/missing.

        """
        self.heart = Heart(self.priority_timer, self.event_dispatcher)
        self.heart.start()

    def stop(self):
        """Stop consuming.

        Does not close the broker connection, so be sure to call
        :meth:`close_connection` when you are finished with it.

        """
        # Notifies other threads that this instance can't be used
        # anymore.
        self._state = CLOSE
        self.should_stop = True
        self.debug("Stopping consumers...")
        self.stop_consumers(close_connection=False)

    def get_logger(self):
        return self._logger

    def close_connection(self):
        self.should_stop = True

    @property
    def information(self):
        """Returns information about this consumer instance
        as a dict.

        This is also the consumer related info returned by
        ``celeryctl stats``.

        """
        conninfo = {}
        if self.connection:
            conninfo = self.connection.info()
            conninfo.pop("password", None)  # don't send password.
        return {"broker": conninfo, "prefetch_count": self.qos.value}

    @property
    def connect_max_retries(self):
        return self.app.conf.BROKER_CONNECTION_MAX_RETRIES

from __future__ import absolute_import
from __future__ import with_statement

from contextlib import contextmanager

from ..events import EventReceiver, EventDispatcher


class Events(object):

    def __init__(self, app=None):
        self.app = app

    def Receiver(self, connection, handlers=None, routing_key="#",
            node_id=None):
        return EventReceiver(connection,
                             handlers=handlers,
                             routing_key=routing_key,
                             node_id=node_id,
                             app=self.app)

    def Dispatcher(self, connection=None, hostname=None, enabled=True,
            channel=None, buffer_while_offline=True):
        return EventDispatcher(connection,
                               hostname=hostname,
                               enabled=enabled,
                               channel=channel,
                               app=self.app)

    def State(self):
        from .state import State as _State
        return _State()

    @contextmanager
    def default_dispatcher(self, hostname=None, enabled=True,
            buffer_while_offline=False):
        with self.app.amqp.producer_pool.acquire(block=True) as prod:
            with self.Dispatcher(prod.connection, hostname, enabled,
                                 prod.channel, buffer_while_offline) as d:
                yield d

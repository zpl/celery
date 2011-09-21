from cl.g import blocking, Event, spawn
from eventlet import Timeout


class AgentWrapper(object):
    g = None

    def __init__(self, agent):
        self.agent = agent
        self._exit_event = Event()

    def start(self):
        if self.g is None:
            g = self.g = spawn(self.agent.run)
            g.link(self._on_exit)
            return g
        raise Exception("thread already started")

    def _on_exit(self, g):
        self._exit_event.send()

    def join(self, timeout=None):
        with Timeout(timeout):
            blocking(self._exit_event.wait)

    def stop(self, join=True, timeout=1e100):
        self.agent.should_stop = True
        if join:
            self.join(timeout)


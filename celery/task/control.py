from ..local import Proxy
from ..utils import instantiate


_default_control = Proxy(lambda: instantiate("celery.app.control.Control"))
broadcast = Proxy(lambda: _default_control.broadcast)
rate_limit = Proxy(lambda: _default_control.rate_limit)
time_limit = Proxy(lambda: _default_control.time_limit)
ping = Proxy(lambda: _default_control.ping)
revoke = Proxy(lambda: _default_control.revoke)
discard_all = Proxy(lambda: _default_control.discard_all)
inspect = Proxy(lambda: _default_control.inspect)

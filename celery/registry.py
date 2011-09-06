"""celery.registry"""
from . import current_app
from .local import Proxy

tasks = Proxy(lambda: current_app.tasks)

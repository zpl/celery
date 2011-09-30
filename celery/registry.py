"""

celery.registry
===============

Registry of available tasks.

"""
from __future__ import absolute_import

from . import current_app
from .local import Proxy

__all__ = ["tasks"]

tasks = Proxy(lambda: current_app.tasks)

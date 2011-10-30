# -*- coding: utf-8 -*-
"""
    celery.registry
    ~~~~~~~~~~~~~~~

    Registry of available tasks.
    See module :mod:`celery.app.registry`.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from . import current_app
from .local import Proxy

tasks = Proxy(lambda: current_app.tasks)

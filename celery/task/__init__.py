# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..app import app_or_default

from .base import Task, PeriodicTask
from .sets import TaskSet, subtask
from .chords import chord
from .control import discard_all   # XXX deprecate

__all__ = ["Task", "TaskSet", "PeriodicTask", "subtask",
           "discard_all", "chord"]


def task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    **Examples**

    .. code-block:: python

        @task
        def refresh_feed(url):
            return Feed.objects.get(url=url).refresh()

    With setting extra options and using retry.

    .. code-block:: python

        @task(max_retries=10)
        def refresh_feed(url):
            try:
                return Feed.objects.get(url=url).refresh()
            except socket.error, exc:
                refresh_feed.retry(exc=exc)

    Calling the resulting task:

            >>> refresh_feed("http://example.com/rss") # Regular
            <Feed: http://example.com/rss>
            >>> refresh_feed.delay("http://example.com/rss") # Async
            <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>
    """
    return app_or_default().task(*args, **kwargs)


def periodic_task(*args, **options):
    """Decorator to create a task class out of any callable.

        .. admonition:: Examples

            .. code-block:: python

                @task
                def refresh_feed(url):
                    return Feed.objects.get(url=url).refresh()

            With setting extra options and using retry.

            .. code-block:: python

                @task(exchange="feeds")
                def refresh_feed(url):
                    try:
                        return Feed.objects.get(url=url).refresh()
                    except socket.error, exc:
                        refresh_feed.retry(exc=exc)

            Calling the resulting task:

                >>> refresh_feed("http://example.com/rss") # Regular
                <Feed: http://example.com/rss>
                >>> refresh_feed.delay("http://example.com/rss") # Async
                <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>

    """
    return task(**dict({"base": PeriodicTask}, **options))


@task(name="celery.backend_cleanup")
def backend_cleanup():
    backend_cleanup.backend.cleanup()

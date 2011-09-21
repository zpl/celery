"""celery.registry"""
from __future__ import absolute_import

import inspect

from UserDict import UserDict

from ..exceptions import NotRegistered


class TaskRegistry(UserDict):
    data = {}

    NotRegistered = NotRegistered

    def __init__(self, *args, **kwargs):
        self.app = kwargs.pop("app", None)

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        self[task.name] = inspect.isclass(task) and task() or task

    def unregister(self, name):
        """Unregister task by name.

        :param name: name of the task to unregister, or a
            :class:`celery.task.base.Task` with a valid `name` attribute.

        :raises celery.exceptions.NotRegistered: if the task has not
            been registered.

        """
        try:
            # Might be a task class
            name = name.name
        except AttributeError:
            pass
        self.pop(name)

    def __getitem__(self, key):
        try:
            return self.data[key]
        except KeyError:
            raise self.NotRegistered(key)

    def pop(self, key, *args):
        try:
            return self.data.pop(self, key, *args)
        except KeyError:
            raise self.NotRegistered(key)


def _unpickle_task(name):
    from .. import current_app
    return current_app.tasks[name]

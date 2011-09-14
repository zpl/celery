from __future__ import absolute_import
from __future__ import with_statement

import imp as _imp
import importlib
import os
import sys

from contextlib import contextmanager

__all__ = ["get_full_cls_name",
           "get_cls_by_name",
           "instantiate",
           "cwd_in_path",
           "find_module",
           "import_from_cwd"]


def get_full_cls_name(cls):
    """With a class, get its full module and class name."""
    return ".".join([cls.__module__,
                     cls.__name__])


def get_cls_by_name(name, aliases={}, imp=None, package=None, **kwargs):
    """Get class by name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        celery.concurrency.processes.TaskPool
                                    ^- class name

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> get_cls_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> get_cls_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> get_cls_by_name(TaskPool) is TaskPool
        True

    """
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, basestring):
        return name                                 # already a class

    name = aliases.get(name) or name
    module_name, _, cls_name = name.rpartition(".")
    if not module_name and package:
        module_name = package
    try:
        module = imp(module_name, package=package, **kwargs)
    except ValueError, exc:
        raise ValueError("Couldn't import %r: %s" % (name, exc))
    return getattr(module, cls_name)


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`get_cls_by_name`.

    """
    return get_cls_by_name(name)(*args, **kwargs)


@contextmanager
def cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:
                pass


def find_module(module, path=None, imp=None):
    """Version of :func:`imp.find_module` supporting dots."""
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        if "." in module:
            last = None
            parts = module.split(".")
            for i, part in enumerate(parts[:-1]):
                path = imp(".".join(parts[:i + 1])).__path__
                last = _imp.find_module(parts[i + 1], path)
            return last
        return _imp.find_module(module)


def import_from_cwd(module, imp=None, package=None):
    """Import module, but make sure it finds modules
    located in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        return imp(module, package=package)

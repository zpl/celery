from __future__ import absolute_import, with_statement

from functools import wraps
from threading import Lock

try:
    from collections import Sequence
except ImportError:
    # <= Py2.5
    Sequence = (list, tuple)  # noqa

from celery.datastructures import LRUCache

KEYWORD_MARK = object()

__all__ = ["firstmethod",
           "padlist",
           "promise",
           "mpromise",
           "maybe_list",
           "maybe_promise",
           "noop",
           "memoize"]


def maybe_list(l):
    if isinstance(l, Sequence):
        return l
    return [l]


def padlist(container, size, default=None):
    """Pad list with default elements.

    Examples:

        >>> first, last, city = padlist(["George", "Costanza", "NYC"], 3)
        ("George", "Costanza", "NYC")
        >>> first, last, city = padlist(["George", "Costanza"], 3)
        ("George", "Costanza", None)
        >>> first, last, city, planet = padlist(["George", "Costanza",
                                                 "NYC"], 4, default="Earth")
        ("George", "Costanza", "NYC", "Earth")

    """
    return list(container)[:size] + [default] * (size - len(container))


class promise(object):
    """A promise.

    Evaluated when called or if the :meth:`evaluate` method is called.
    The function is evaluated on every access, so the value is not
    memoized (see :class:`mpromise`).

    Overloaded operations that will evaluate the promise:
        :meth:`__str__`, :meth:`__repr__`, :meth:`__cmp__`.

    """

    def __init__(self, fun, *args, **kwargs):
        self._fun = fun
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self.evaluate()

    def evaluate(self):
        return self._fun(*self._args, **self._kwargs)

    def __str__(self):
        return str(self())

    def __repr__(self):
        return repr(self())

    def __cmp__(self, rhs):
        if isinstance(rhs, self.__class__):
            return -cmp(rhs, self())
        return cmp(self(), rhs)

    def __eq__(self, rhs):
        return self() == rhs

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    def __reduce__(self):
        return (self.__class__, (self._fun, ), {"_args": self._args,
                                                "_kwargs": self._kwargs})


class mpromise(promise):
    """Memoized promise.

    The function is only evaluated once, every subsequent access
    will return the same value.

    .. attribute:: evaluated

        Set to to :const:`True` after the promise has been evaluated.

    """
    evaluated = False
    _value = None

    def evaluate(self):
        if not self.evaluated:
            self._value = super(mpromise, self).evaluate()
            self.evaluated = True
        return self._value


def maybe_promise(value):
    """Evaluates if the value is a promise."""
    if isinstance(value, promise):
        return value.evaluate()
    return value


def noop(*args, **kwargs):
    """No operation.

    Takes any arguments/keyword arguments and does nothing.

    """
    pass


def firstmethod(method):
    """Returns a functions that with a list of instances,
    finds the first instance that returns a value for the given method.

    The list can also contain promises (:class:`promise`.)

    """

    def _matcher(seq, *args, **kwargs):
        for cls in seq:
            try:
                answer = getattr(maybe_promise(cls), method)(*args, **kwargs)
                if answer is not None:
                    return answer
            except AttributeError:
                pass
    return _matcher


def memoize(maxsize=None, Cache=LRUCache):

    def _memoize(fun):
        mutex = Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs):
            key = args + (KEYWORD_MARK, ) + tuple(sorted(kwargs.iteritems()))
            try:
                with mutex:
                    value = cache[key]
            except KeyError:
                value = fun(*args, **kwargs)
                _M.misses += 1
                with mutex:
                    cache[key] = value
            else:
                _M.hits += 1
            return value

        def clear():
            """Clear the cache and reset cache statistics."""
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize

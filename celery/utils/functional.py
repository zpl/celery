from __future__ import absolute_import, with_statement

from functools import wraps
from threading import Lock

from kombu.utils.functional import promise, maybe_promise

from ..datastructures import LRUCache

KEYWORD_MARK = object()

__all__ = ["firstmethod",
           "padlist",
           "promise",
           "mpromise",
           "maybe_promise",
           "noop",
           "memoize"]


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

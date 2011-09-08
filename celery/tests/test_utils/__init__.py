import pickle
from celery.tests.utils import unittest

from celery import utils
from celery.utils.functional import promise, mpromise, maybe_promise
from celery.utils.functional import padlist, firstmethod


def double(x):
    return x * 2


class test_utils(unittest.TestCase):

    def test_padlist(self):
        self.assertListEqual(padlist(["George", "Costanza", "NYC"], 3),
                ["George", "Costanza", "NYC"])
        self.assertListEqual(padlist(["George", "Costanza"], 3),
                ["George", "Costanza", None])
        self.assertListEqual(padlist(["George", "Costanza", "NYC"], 4,
                                           default="Earth"),
                ["George", "Costanza", "NYC", "Earth"])

    def test_firstmethod_AttributeError(self):
        self.assertIsNone(firstmethod("foo")([object()]))

    def test_firstmethod_promises(self):

        class A(object):

            def __init__(self, value=None):
                self.value = value

            def m(self):
                return self.value

        self.assertEqual("four", firstmethod("m")([
            A(), A(), A(), A("four"), A("five")]))
        self.assertEqual("four", firstmethod("m")([
            A(), A(), A(), promise(lambda: A("four")), A("five")]))

    def test_truncate_text(self):
        self.assertEqual(utils.truncate_text("ABCDEFGHI", 3), "ABC...")
        self.assertEqual(utils.truncate_text("ABCDEFGHI", 10), "ABCDEFGHI")

    def test_abbr(self):
        self.assertEqual(utils.abbr(None, 3), "???")
        self.assertEqual(utils.abbr("ABCDEFGHI", 6), "ABC...")
        self.assertEqual(utils.abbr("ABCDEFGHI", 20), "ABCDEFGHI")
        self.assertEqual(utils.abbr("ABCDEFGHI", 6, None), "ABCDEF")

    def test_abbrtask(self):
        self.assertEqual(utils.abbrtask(None, 3), "???")
        self.assertEqual(utils.abbrtask("feeds.tasks.refresh", 10),
                                        "[.]refresh")
        self.assertEqual(utils.abbrtask("feeds.tasks.refresh", 30),
                                        "feeds.tasks.refresh")

    def test_cached_property(self):

        def fun(obj):
            return fun.value

        x = utils.cached_property(fun)
        self.assertIs(x.__get__(None), x)
        self.assertIs(x.__set__(None, None), x)
        self.assertIs(x.__delete__(None), x)


class test_promise(unittest.TestCase):

    def test__str__(self):
        self.assertEqual(str(promise(lambda: "the quick brown fox")),
                "the quick brown fox")

    def test__repr__(self):
        self.assertEqual(repr(promise(lambda: "fi fa fo")),
                "'fi fa fo'")

    def test_evaluate(self):
        self.assertEqual(promise(lambda: 2 + 2)(), 4)
        self.assertEqual(promise(lambda x: x * 4, 2), 8)
        self.assertEqual(promise(lambda x: x * 8, 2)(), 16)

    def test_cmp(self):
        self.assertEqual(promise(lambda: 10), promise(lambda: 10))
        self.assertNotEqual(promise(lambda: 10), promise(lambda: 20))

    def test__reduce__(self):
        x = promise(double, 4)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(x(), y())

    def test__deepcopy__(self):
        from copy import deepcopy
        x = promise(double, 4)
        y = deepcopy(x)
        self.assertEqual(x._fun, y._fun)
        self.assertEqual(x._args, y._args)
        self.assertEqual(x(), y())


class test_mpromise(unittest.TestCase):

    def test_is_memoized(self):

        it = iter(xrange(20, 30))
        p = mpromise(it.next)
        self.assertEqual(p(), 20)
        self.assertTrue(p.evaluated)
        self.assertEqual(p(), 20)
        self.assertEqual(repr(p), "20")


class test_maybe_promise(unittest.TestCase):

    def test_evaluates(self):
        self.assertEqual(maybe_promise(promise(lambda: 10)), 10)
        self.assertEqual(maybe_promise(20), 20)

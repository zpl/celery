from celery.utils.imports import get_full_cls_name, get_cls_by_name
from celery.tests.utils import unittest


class test_import_utils(unittest.TestCase):

    def test_get_full_cls_name(self):
        Class = type("Fox", (object, ), {"__module__": "quick.brown"})
        self.assertEqual(get_full_cls_name(Class), "quick.brown.Fox")

    def test_get_cls_by_name__instance_returns_instance(self):
        instance = object()
        self.assertIs(get_cls_by_name(instance), instance)

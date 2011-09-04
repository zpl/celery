import warnings

from celery.task import backend_cleanup
from celery.tests.utils import unittest


class test_backend_cleanup(unittest.TestCase):

    def test_run(self):
        backend_cleanup.apply()

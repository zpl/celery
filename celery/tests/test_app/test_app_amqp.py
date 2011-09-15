from __future__ import with_statement

from kombu import Exchange, Queue
from kombu import pools
from mock import Mock

from celery.tests.utils import AppCase

from celery.app.amqp import MSG_OPTIONS, extract_msg_options


class TestMsgOptions(AppCase):

    def test_MSG_OPTIONS(self):
        self.assertTrue(MSG_OPTIONS)

    def test_extract_msg_options(self):
        testing = {"mandatory": True, "routing_key": "foo.xuzzy"}
        result = extract_msg_options(testing)
        self.assertEqual(result["mandatory"], True)
        self.assertEqual(result["routing_key"], "foo.xuzzy")


class test_TaskProducer(AppCase):

    def test__exit__(self):

        producer = self.app.amqp.TaskProducer(self.app.broker_connection())
        producer.release = Mock()
        with producer:
            pass
        producer.release.assert_called_with()

    def test_ensure_declare_queue(self, q="x1242112"):
        producer = self.app.amqp.TaskProducer(Mock())
        self.app.amqp.queues.add(Queue(q, Exchange(q), q))
        producer._declare_queue(q, retry=True)
        self.assertTrue(producer.connection.ensure.call_count)

    def test_ensure_declare_exchange(self, e=Exchange("x9248311")):
        producer = self.app.amqp.TaskProducer(Mock())
        producer._declare_exchange(e, retry=True)
        self.assertTrue(producer.connection.ensure.call_count)

    def test_retry_policy(self):
        pub = self.app.amqp.TaskProducer(Mock())
        pub.send_task("tasks.add", (2, 2), {},
                       retry_policy={"frobulate": 32.4})

    def test_publish_no_retry(self):
        pub = self.app.amqp.TaskProducer(Mock())
        pub.send_task("tasks.add", (2, 2), {}, retry=False, chord=123)
        self.assertFalse(pub.connection.ensure.call_count)


class test_ProducerPool(AppCase):

    def test_setup_nolimit(self):
        pools.reset()
        conf = self.app.conf
        prev, conf.BROKER_POOL_LIMIT = conf.BROKER_POOL_LIMIT, None
        pools.set_limit(None)
        try:
            pool = self.app.amqp.producers[self.app.broker_connection()]
            self.assertEqual(pool.limit, 0)
            self.assertFalse(pool._resource.queue)

            r1 = pool.acquire()
            r2 = pool.acquire()
            r1.release()
            r2.release()
            r1 = pool.acquire()
            r2 = pool.acquire()
        finally:
            conf.BROKER_POOL_LIMIT = prev
            pools.set_limit(prev)

    def test_setup(self):
        pools.reset()
        conf = self.app.conf
        prev, conf.BROKER_POOL_LIMIT = conf.BROKER_POOL_LIMIT, 10
        pools.set_limit(10)
        try:
            pool = self.app.amqp.producers[self.app.broker_connection()]
            self.assertEqual(pool.limit, 10)
            self.assertTrue(pool._resource.queue)

            p1 = r1 = pool.acquire()
            p2 = r2 = pool.acquire()
            r1.release()
            r2.release()
            r1 = pool.acquire()
            r2 = pool.acquire()
            self.assertIs(p2, r1)
            self.assertIs(p1, r2)
            r1.release()
            r2.release()
        finally:
            conf.BROKER_POOL_LIMIT = prev
            pools.set_limit(prev)

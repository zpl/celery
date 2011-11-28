from __future__ import absolute_import
from __future__ import with_statement

from celery import current_app
from celery import result
from celery.task import chords
from celery.task import sets
from celery.tests.utils import AppCase, Mock


def passthru(x):
    return x


@current_app.task
def add(x, y):
    return x + y


@current_app.task
def callback(r):
    return r


class TSR(result.TaskSetResult):
    is_ready = True
    value = [2, 4, 8, 6]

    def ready(self):
        return self.is_ready

    def join(self, **kwargs):
        return self.value

    def join_native(self, **kwargs):
        return self.value


class test_unlock_chord_task(AppCase):

    def test_unlock_ready(self):
        callback.apply_async = Mock()

        unlock = self.app.tasks["celery.chord_unlock"]
        retry = unlock.retry = Mock()

        body = callback.subtask()
        pts, result.TaskSetResult = result.TaskSetResult, TSR
        subtask, sets.subtask = sets.subtask, passthru
        try:
            unlock("setid", body, result=[1, 2, 3])
        finally:
            sets.subtask = subtask
            result.TaskSetResult = pts
        callback.apply_async.assert_called_with(([2, 4, 8, 6], ), {})
        # did not retry
        self.assertFalse(retry.call_count)

    def test_when_not_ready(self):
        callback.apply_async = Mock()
        unlock = self.app.tasks["celery.chord_unlock"]
        retry = unlock.retry = Mock()

        class NeverReady(TSR):
            is_ready = False

        pts, result.TaskSetResult = result.TaskSetResult, NeverReady
        try:
            unlock("setid", callback.subtask, interval=10,
                            max_retries=30, result=[1, 2, 3])
            self.assertFalse(callback.apply_async.call_count)
            # did retry
            retry.assert_called_with(countdown=10,
                                     max_retries=30)
        finally:
            result.TaskSetResult = pts

    def test_is_in_registry(self):
        self.assertIn("celery.chord_unlock", current_app.tasks)


class test_chord(AppCase):

    def test_apply(self):
        Chord = Mock()

        class chord(chords.chord):

            def __init__(self, *args, **kwargs):
                super(chord, self).__init__(*args, **kwargs)
                self.Chord = Chord

        x = chord(add.subtask((i, i)) for i in xrange(10))
        body = add.subtask((2, ))
        result = x(body)
        self.assertEqual(result.task_id, body.options["task_id"])
        self.assertTrue(Chord.apply_async.call_count)


class test_Chord_task(AppCase):

    def test_run(self):
        Chord = self.app.tasks["celery.chord"].__class__

        class Chord(Chord):
            backend = Mock()

        body = dict()
        Chord()(self.app.TaskSet(add.subtask((i, i))
                                    for i in xrange(5)), body)
        Chord()([add.subtask((i, i)) for i in xrange(5)], body)
        self.assertEqual(Chord.backend.on_chord_apply.call_count, 2)

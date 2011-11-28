from __future__ import absolute_import

from celery.utils import uuid

_builtins = []


def builtin_task(constructor):
    _builtins.append(constructor)
    return constructor


@builtin_task
def add_backend_cleanup_task(app):

    @app.task(name="celery.backend_cleanup")
    def backend_cleanup():
        backend_cleanup.backend.cleanup()

    return backend_cleanup


@builtin_task
def add_unlock_chord_task(app):

    @app.task(name="celery.chord_unlock", max_retries=None)
    def _unlock_chord(setid, callback, interval=1, propagate=False,
            max_retries=None, result=None):
        from ...result import AsyncResult, TaskSetResult
        from ...task.sets import subtask

        result = TaskSetResult(setid, map(AsyncResult, result))
        if result.ready():
            j = result.join
            if result.supports_native_join:
                j = result.join_native
            subtask(callback).delay(j(propagate=propagate))
        else:
            _unlock_chord.retry(countdown=interval, max_retries=max_retries)


@builtin_task
def add_Chord_task(app):
    from ...task.sets import TaskSet

    class Chord(app.Task):
        name = "celery.chord"

        def run(self, set, body, interval=1, max_retries=None,
                propagate=False, **kwargs):
            if not isinstance(set, TaskSet):
                set = self.app.TaskSet(set)
            r = []
            setid = uuid()
            for task in set.tasks:
                tid = uuid()
                task.options.update(task_id=tid, chord=body)
                r.append(self.app.AsyncResult(tid))
            self.backend.on_chord_apply(setid, body,
                                        interval=interval,
                                        max_retries=max_retries,
                                        propagate=propagate,
                                        result=r)
            return set.apply_async(taskset_id=setid)


def load_builtins(app):
    for constructor in _builtins:
        constructor(app)

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
            max_retries=None):
        from ...result import TaskSetResult
        from ...task.sets import subtask
        result = TaskSetResult.restore(setid, backend=app.backend)
        if result.ready():
            subtask(callback).delay(result.join(propagate=propagate))
            result.delete()
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
            self.app.TaskSetResult(setid, r).save()
            self.backend.on_chord_apply(setid, body, interval,
                                        max_retries=max_retries,
                                        propagate=propagate)
            return set.apply_async(taskset_id=setid)


def load_builtins(app):
    for constructor in _builtins:
        constructor(app)

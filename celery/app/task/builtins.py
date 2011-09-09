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
        result = TaskSetResult.restore(setid)
        if result.ready():
            subtask(callback).delay(result.join(propagate=propagate))
            result.delete()
        else:
            _unlock_chord.retry(countdown=interval, max_retries=max_retries)


def load_builtins(app):
    for constructor in _builtins:
        constructor(app)

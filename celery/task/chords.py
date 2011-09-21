from __future__ import absolute_import

from ..app import app_or_default
from ..utils import uuid


class chord(object):

    def __init__(self, tasks, app=None, **options):
        self.app = app_or_default(app)
        self.tasks = tasks
        self.options = options
        self.Chord = self.app.tasks["celery.chord"]

    def __call__(self, body, **options):
        tid = body.options.setdefault("task_id", uuid())
        self.Chord.apply_async((list(self.tasks), body), self.options,
                                **options)
        return body.type.app.AsyncResult(tid)

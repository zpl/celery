====================================================
 celery.app.control
====================================================

.. contents::
    :local:
.. currentmodule:: celery.app.control

.. automodule:: celery.app.control

    .. automethod:: inspect

    .. appattr:: celery.control.purge

    celery.control.purge
    ~~~~~~~~~~~~~~~~~~~~

    Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    .. appattr:: celery.control.revoke

    celery.control.revoke
    ~~~~~~~~~~~~~~~~~~~~~

    Revoke a task by id.

    If a task is revoked, the workers will ignore the task and
    not execute it after all.

    :param task_id: Id of the task to revoke.
    :keyword terminate: Also terminate the process currently working
        on the task (if any).
    :keyword signal: Name of signal to send to process if terminate.
        Default is TERM.
    :keyword destination: If set, a list of the hosts to send the
        command to, when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    .. appattr:: celery.control.ping

    celery.control.ping
    ~~~~~~~~~~~~~~~~~~~

    Ping workers.

    Returns answer from alive workers.

    :keyword destination: If set, a list of the hosts to send the
        command to, when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    .. appattr:: celery.control.rate_limit

    celery.control.rate_limit
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Set rate limit for task by type.

    :param task_name: Name of task to change rate limit for.
    :param rate_limit: The rate limit as tasks per second, or a rate limit
        string (`"100/m"`, etc.
        see :attr:`celery.task.base.Task.rate_limit` for
        more information).
    :keyword destination: If set, a list of the hosts to send the
        command to, when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    .. appattr:: celery.control.time_limit

    celery.control.time_limit
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Set time limits for task by type.

    :param task_name: Name of task to change time limits for.
    :keyword soft: New soft time limit (in seconds).
    :keyword hard: New hard time limit (in seconds).

    Any additional keyword arguments are passed on to :meth:`broadcast`.


    .. appattr:: celery.control.broadcast

    celery.control.broadcast
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Broadcast a control command to the celery workers.

    :param command: Name of command to send.
    :param arguments: Keyword arguments for the command.
    :keyword destination: If set, a list of the hosts to send the
        command to, when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.
    :keyword callback: Callback called immediately for each reply
        received.

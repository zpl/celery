# -*- coding: utf-8 -*-
"""
    celery.utils.encoding
    ~~~~~~~~~~~~~~~~~~~~~

    Utilities to encode text, and to safely emit text from running
    applications without crashing with the infamous :exc:`UnicodeDecodeError`
    exception.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from kombu.utils.encoding import (safe_str, safe_repr,  # noqa
                                  default_encoding, from_utf8,
                                  str_to_bytes, bytes_to_str, str_t,
                                  bytes_t, ensure_bytes)

"""

celery.utils.encoding
=====================

Utilties to encode text, and to safely emit text from running
applications without crashing with the infamous :exc:`UnicodeDecodeError`
exception.

"""
from __future__ import absolute_import

from kombu.utils.encoding import (safe_str, safe_repr,  # noqa
                                  default_encoding, from_utf8,
                                  str_to_bytes, bytes_to_str)

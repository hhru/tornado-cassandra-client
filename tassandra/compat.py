# coding=utf-8

import sys

__all__ = [
    'string_types'
]

PY3 = sys.version_info >= (3,)

if PY3:
    string_types = str
    text_type = str

    def iteritems(d, **kw):
        return d.items(**kw)
else:
    string_types = basestring
    text_type = unicode

    def iteritems(d, **kw):
        return d.iteritems(**kw)

# coding=utf-8

import sys

PY3 = sys.version_info >= (3,)

if PY3:
    def itervalues(d, **kw):
        return iter(d.values(**kw))
else:
    def itervalues(d, **kw):
        return d.itervalues(**kw)

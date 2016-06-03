# coding=utf-8

import io
import sys
import inspect

from tassandra.protocol.internal import Message, serialize_stringmap, deserialize_int, deserialize_string,\
    serialize_longstring, serialize_short, serialize_byte, serialize_long, deserialize_short, deserialize_value


class ConsistencyLevel(object):
    ANY = 0
    ONE = 1
    TWO = 2
    THREE = 3
    QUORUM = 4
    ALL = 5
    LOCAL_QUORUM = 6
    EACH_QUORUM = 7
    SERIAL = 8
    LOCAL_SERIAL = 9
    LOCAL_ONE = 10


class ErrorMessage(Message):
    opcode = 0x0

    def __init__(self, code, message):
        pass

    @classmethod
    def deserialize(cls, buf):
        code = deserialize_int(buf)
        msg = deserialize_string(buf)
        return cls(code=code, message=msg)


class ReadyMessage(Message):
    opcode = 0x02
    name = 'READY'

    @classmethod
    def deserialize(cls, buf):
        return cls()


class StartupMessage(Message):
    opcode = 0x01
    name = 'STARTUP'
    DEFAULT_CQL_VERSION = '3.0.0'

    OPTIONS = {
        'CQL_VERSION': DEFAULT_CQL_VERSION,
    }

    def serialize_body(self, buf):
        serialize_stringmap(buf, self.OPTIONS)


class QueryMessage(Message):
    opcode = 0x07
    name = 'QUERY'

    def __init__(self, query, consistency_level, serial_consistency_level=None, timestamp=None):
        self.query = query
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.timestamp = timestamp

    def serialize_body(self, buf):
        serialize_longstring(buf, self.query)
        serialize_short(buf, self.consistency_level)

        flags = 0x00

        if self.serial_consistency_level is not None:
            flags |= 0x10
        if self.timestamp is not None:
            flags |= 0x20

        serialize_byte(buf, flags)

        if self.serial_consistency_level is not None:
            serialize_short(buf, self.serial_consistency_level)
        if self.timestamp is not None:
            serialize_long(buf, self.timestamp)


class ResultMessage(Message):
    opcode = 0x08
    name = 'RESULT'

    _type_codes = {
        0x0001: lambda value: value.decode('ascii'),
        0x000A: lambda value: value.decode('utf8'),
        0x000D: lambda value: value.decode('utf8'),
    }

    _FLAGS_GLOBAL_TABLES_SPEC = 0x0001

    def __init__(self, kind, results):
        self.kind = kind
        self.results = results

    @classmethod
    def deserialize(cls, buf):
        kind = deserialize_int(buf)

        if kind == 1:
            results = None
        elif kind == 2:
            results = cls.deserialize_rows(buf)
        else:
            raise Exception('Unsupported result type: %d', kind)

        return cls(kind, results)

    @classmethod
    def deserialize_rows(cls, buf):
        column_names, column_types = cls.deserialize_metadata(buf)
        parsed_rows = []

        for _ in xrange(deserialize_int(buf)):
            parsed_rows.append(tuple(cls._type_codes.get(type_id)(deserialize_value(buf)) for type_id in column_types))

        return column_names, parsed_rows

    @classmethod
    def deserialize_metadata(cls, buf):
        flags = deserialize_int(buf)
        columns = deserialize_int(buf)
        global_tables_spec = bool(flags & 0x0001)

        if global_tables_spec:
            deserialize_string(buf)
            deserialize_string(buf)

        column_names, column_types = ([], [])

        for _ in range(columns):
            if not global_tables_spec:
                deserialize_string(buf)
                deserialize_string(buf)

            column_names.append(deserialize_string(buf))
            column_types.append(cls.deserialize_type(buf))

        return column_names, column_types

    @classmethod
    def deserialize_type(cls, buf):
        optid = deserialize_short(buf)

        '''try:
            typeclass = cls._type_codes[optid]
        except KeyError:
            raise NotSupportedError("Unknown data type code 0x%04x. Have to skip"
                                    " entire result set." % (optid,))

        if typeclass in (ListType, SetType):
            subtype = cls.deserialize_type(f)
            typeclass = typeclass.apply_parameters((subtype,))
        elif typeclass == MapType:
            keysubtype = cls.deserialize_type(f)
            valsubtype = cls.deserialize_type(f)
            typeclass = typeclass.apply_parameters((keysubtype, valsubtype))
        elif typeclass == TupleType:
            num_items = read_short(f)
            types = tuple(cls.deserialize_type(f) for _ in xrange(num_items))
            typeclass = typeclass.apply_parameters(types)'''

        return optid


MAPPING = {cls.opcode: cls for (name, cls) in
           inspect.getmembers(sys.modules[__name__], lambda obj: inspect.isclass(obj) and issubclass(obj, Message))}


def parse_response(stream_id, flags, opcode, body):

    #if flags:
    #    log.warning("Unknown protocol flags set: %02x. May cause problems.", flags)

    response_class = MAPPING.get(opcode)
    response = response_class.deserialize(io.BytesIO(body))

    return response

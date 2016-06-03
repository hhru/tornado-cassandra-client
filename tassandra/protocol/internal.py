# coding=utf-8

import io
import struct

from tassandra.compat import text_type, iteritems


# version | flags | stream | opcode | length
_header_struct = struct.Struct('>BBhBi')

_short_struct = struct.Struct('>H')
_int_struct = struct.Struct('>i')
_byte_struct = struct.Struct('>b')
_long_struct = struct.Struct('>Q')


parse_header = _header_struct.unpack
combine_header = _header_struct.pack


def serialize_header(buf, version, flags, stream, opcode, length):
    buf.write(combine_header(version, flags, stream, opcode, length))


def serialize_short(buf, value):
    buf.write(_short_struct.pack(value))


def deserialize_short(buf):
    return _short_struct.unpack(buf.read(2))[0]


def serialize_string(buf, value):
    if isinstance(value, text_type):
        value = value.encode('utf8')
    serialize_short(buf, len(value))
    buf.write(value)


def serialize_stringmap(buf, stringmap):
    serialize_short(buf, len(stringmap))

    for k, v in iteritems(stringmap):
        serialize_string(buf, k)
        serialize_string(buf, v)


def serialize_int(buf, value):
    buf.write(_int_struct.pack(value))


def deserialize_int(buf):
    return _int_struct.unpack(buf.read(4))[0]


def deserialize_string(buf):
    contents = buf.read(deserialize_short(buf))
    return contents.decode('utf8')


def serialize_longstring(buf, value):
    if isinstance(value, text_type):
        value = value.encode('utf8')
    serialize_int(buf, len(value))
    buf.write(value)


def serialize_byte(buf, value):
    buf.write(_byte_struct.pack(value))


def serialize_long(buf, value):
    buf.write(_long_struct.pack(value))


def deserialize_value(buf):
    size = deserialize_int(buf)

    if size < 0:
        return None

    return buf.read(size)


class Message(object):
    opcode = 0x0

    def serialize(self, stream_id, protocol_version, compression=None):
        body = io.BytesIO()
        self.serialize_body(body)
        body = body.getvalue()

        message = io.BytesIO()
        serialize_header(message, protocol_version, 0, stream_id, self.opcode, len(body))
        message.write(body)

        return message.getvalue()

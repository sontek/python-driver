try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from .loading import ccassandra

try:
    import gc
except ImportError:
    gc = None

import platform
import six
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from cassandra.cqltypes import (
    lookup_casstype,
    AsciiType,
    IntegerType,
    TupleType,
    UserType,
)
from cassandra.util import OrderedMap, sortedset


class TestUserType(object):
    """Test-specific user type.
    """

    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

    def __eq__(self, other):
        return \
            isinstance(other, self.__class__) and \
            self.a == other.a and \
            self.b == other.b and \
            self.c == other.c


test_user_type_udt_unmapped = UserType.make_udt_class(
    keyspace='ks1',
    udt_name='test_user_type_unmapped',
    names_and_types=[
        ('a', IntegerType),
        ('b', AsciiType),
        ('c', TupleType.apply_parameters([IntegerType, AsciiType])),
    ],
    mapped_class=None
)


test_user_type_udt_mapped = UserType.make_udt_class(
    keyspace='ks1',
    udt_name='test_user_type_mapped',
    names_and_types=[
        ('a', IntegerType),
        ('b', AsciiType),
        ('c', TupleType.apply_parameters([IntegerType, AsciiType])),
    ],
    mapped_class=TestUserType
)


# TODO: lists, tuples etc. with empty values.
marshalled_value_pairs = (
    # binary form, type, python native type, protocol

    # List.
    (b'\x00\x00\x00\x03\x00\x00\x00\x04\x00\x00\x04\xd2\x00\x00\x00\x04\x00'
     b'\x00\x11\xd7\x00\x00\x00\x04\x00\x014\x90',
     'ListType(Int32Type)',
     [1234, 4567, 78992],
     3),
    (b'\x00\x03\x00\x04\x00\x00\x04\xd2\x00\x04\x00\x00\x11\xd7\x00\x04\x00'
     b'\x014\x90',
     'ListType(Int32Type)',
     [1234, 4567, 78992],
     2),

    # Set.
    (b'\x00\x03\x00\x04\x00\x00\x04\xd2\x00\x04\x00\x00\x11\xd7\x00\x04\x00'
     b'\x014\x90',
     'SetType(Int32Type)',
     sortedset([1234, 4567, 78992]),
     2),
    (b'\x00\x00\x00\x03\x00\x00\x00\x04\x00\x00\x04\xd2\x00\x00\x00\x04\x00'
     b'\x00\x11\xd7\x00\x00\x00\x04\x00\x014\x90',
     'SetType(Int32Type)',
     sortedset([1234, 4567, 78992]),
     3),

    (b'lorem ipsum dolor sit amet', 'AsciiType', 'lorem ipsum dolor sit amet', 3),
    (b'', 'AsciiType', '', 3),
    (b'\x01', 'BooleanType', True, 3),
    (b'\x00', 'BooleanType', False, 3),
    (b'', 'BooleanType', None, 3),
    (b'\xff\xfe\xfd\xfc\xfb', 'BytesType', b'\xff\xfe\xfd\xfc\xfb', 3),
    (b'', 'BytesType', b'', 3),
    (b'\x7f\xff\xff\xff\xff\xff\xff\xff', 'CounterColumnType', 9223372036854775807L, 3),
    (b'\x80\x00\x00\x00\x00\x00\x00\x00', 'CounterColumnType', -9223372036854775808L, 3),
    (b'', 'CounterColumnType', None, 3),
    (b'\x00\x00\x013\x7fb\xeey', 'DateType', datetime(2011, 11, 7, 18, 55, 49, 881000), 3),
    (b'', 'DateType', None, 3),
    (b'\x00\x00\x00\r\nJ\x04"^\x91\x04\x8a\xb1\x18\xfe', 'DecimalType', Decimal('1243878957943.1234124191998'), 3),
    (b'\x00\x00\x00\x06\xe5\xde]\x98Y', 'DecimalType', Decimal('-112233.441191'), 3),
    (b'\x00\x00\x00\x14\x00\xfa\xce', 'DecimalType', Decimal('0.00000000000000064206'), 3),
    (b'\x00\x00\x00\x14\xff\x052', 'DecimalType', Decimal('-0.00000000000000064206'), 3),
    (b'\xff\xff\xff\x9c\x00\xfa\xce', 'DecimalType', Decimal('64206e100'), 3),
    (b'', 'DecimalType', None, 3),
    (b'@\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', 19432.125, 3),
    (b'\xc0\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', -19432.125, 3),
    (b'\x7f\xef\x00\x00\x00\x00\x00\x00', 'DoubleType', 1.7415152243978685e+308, 3),
    (b'', 'DoubleType', None, 3),
    (b'F\x97\xd0@', 'FloatType', 19432.125, 3),
    (b'\xc6\x97\xd0@', 'FloatType', -19432.125, 3),
    (b'\xc6\x97\xd0@', 'FloatType', -19432.125, 3),
    (b'\x7f\x7f\x00\x00', 'FloatType', 338953138925153547590470800371487866880.0, 3),
    (b'', 'FloatType', None, 3),
    (b'\x7f\x50\x00\x00', 'Int32Type', 2135949312, 3),
    (b'\xff\xfd\xcb\x91', 'Int32Type', -144495, 3),
    (b'', 'Int32Type', None, 3),
    (b'f\x1e\xfd\xf2\xe3\xb1\x9f|\x04_\x15', 'IntegerType', 123456789123456789123456789, 3),
    (b'\xfb.', 'IntegerType', -1234, 3),
    (b'', 'IntegerType', None, 3),
    (b'\x7f\xff\xff\xff\xff\xff\xff\xff', 'LongType', 9223372036854775807L, 3),
    (b'\x80\x00\x00\x00\x00\x00\x00\x00', 'LongType', -9223372036854775808L, 3),
    (b'', 'LongType', None, 3),
    (b'', 'InetAddressType', None, 3),
    (b'A46\xa9', 'InetAddressType', '65.52.54.169', 3),
    (b'*\x00\x13(\xe1\x02\xcc\xc0\x00\x00\x00\x00\x00\x00\x01"', 'InetAddressType', '2a00:1328:e102:ccc0::122', 3),
    (b'\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6', 'UTF8Type', u'\u307e\u3057\u3066', 3),
    (b'\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6' * 1000, 'UTF8Type', u'\u307e\u3057\u3066' * 1000, 3),
    (b'', 'UTF8Type', u'', 3),
    (b'\xff' * 16, 'UUIDType', UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), 3),
    (b'I\x15~\xfc\xef<\x9d\xe3\x16\x98\xaf\x80\x1f\xb4\x0b*', 'UUIDType', UUID('49157efc-ef3c-9de3-1698-af801fb40b2a'), 3),
    (b'', 'UUIDType', None, 3),
    (b'', 'MapType(AsciiType, BooleanType)', None, 3),
    (b'\x00\x00\x00\x01\x01', 'TupleType(IntegerType)', (1,), 3),
    (b'\x00\x00\x00\x04\x00\x00\x04\xd2\x00\x00\x00\x01\x01',
     'TupleType(Int32Type, BooleanType)',
     (1234, True), 3),
    (b'', 'ListType(FloatType)', None, 3),
    (b'', 'SetType(LongType)', None, 3),
    (b'\x00\x00', 'MapType(DecimalType, BooleanType)', OrderedMap(), 2),
    (b'\x00\x00', 'ListType(FloatType)', [], 2),
    (b'\x00\x00', 'SetType(IntegerType)', sortedset(), 2),
    (b'\x00\x01\x00\x10\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0', 'ListType(TimeUUIDType)', [UUID(bytes=b'\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0')], 2),
    (b'hello!', 'FrozenType(UTF8Type)', u'hello!', 2),
    (b'hello!', 'FrozenType(UTF8Type)', u'hello!', 3),
    (b'hello!', 'ReversedType(UTF8Type)', u'hello!', 2),
    (b'hello!', 'ReversedType(UTF8Type)', u'hello!', 3),

    # User type.
    (b'\x00\x00\x00\x01\x01\x00\x00\x00\x04food\x00\x00\x00\x0e\x00\x00\x00'
     b'\x02\x01\xec\x00\x00\x00\x04fish',
     test_user_type_udt_unmapped,
     test_user_type_udt_unmapped.tuple_type(1, 'food', (492, 'fish')),
     3),
    (b'\x00\x00\x00\x01\x01\x00\x00\x00\x04food\x00\x00\x00\x0e\x00\x00\x00'
     b'\x02\x01\xec\x00\x00\x00\x04fish',
     test_user_type_udt_mapped,
     TestUserType(1, 'food', (492, 'fish')),
     3),    
)

ordered_map_value = OrderedMap([(u'\u307fbob', 199),
                                (u'', -1),
                                (u'\\', 0)])

# these following entries work for me right now, but they're dependent on
# vagaries of internal python ordering for unordered types
marshalled_value_pairs_unsafe = (
    (b'\x00\x03\x00\x06\xe3\x81\xbfbob\x00\x04\x00\x00\x00\xc7\x00\x00\x00\x04\xff\xff\xff\xff\x00\x01\\\x00\x04\x00\x00\x00\x00', 'MapType(UTF8Type, Int32Type)', ordered_map_value, 2),
    (b'\x00\x02\x00\x08@\x01\x99\x99\x99\x99\x99\x9a\x00\x08@\x14\x00\x00\x00\x00\x00\x00', 'SetType(DoubleType)', sortedset([2.2, 5.0]), 2),
    (b'\x00', 'IntegerType', 0, 3),
)

if platform.python_implementation() == 'CPython':
    # Only run tests for entries which depend on internal python ordering under
    # CPython
    marshalled_value_pairs += marshalled_value_pairs_unsafe


if ccassandra:
    class DeserializeTest(unittest.TestCase):
        def test_deserialize_cqltype(self):
            """ccassandra.deserialize_cqltype(..)
            """

            for ser, cql_type_name, expected, protocol in marshalled_value_pairs:
                # TODO: implement proper handling of None cases.
                if expected is None:
                    continue

                if isinstance(cql_type_name, six.string_types):
                    cql_type = lookup_casstype(cql_type_name)
                else:
                    cql_type = cql_type_name

                try:
                    actual = ccassandra.deserialize_cqltype(ser, cql_type, protocol)
                except Exception as e:
                    self.fail('Deserialization for %s (%s) using protocol %d '
                              'failed with exception %s: %s' %
                              (cql_type_name, cql_type, protocol, type(e), e))

                if gc:
                    gc.collect()

                self.assertEqual(
                    expected,
                    actual,
                    msg='Deserialization for %s (%s) failed: got %r '
                    'instead of %r' %
                    (cql_type_name, cql_type, actual, expected)
                )
                self.assertEqual(
                    type(actual),
                    type(expected),
                    msg='Deserialization for %s (%s) gave wrong type '
                    '(%s instead of %s)' %
                    (cql_type_name, cql_type, type(actual), type(expected))
                )

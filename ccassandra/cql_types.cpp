#include <arpa/inet.h>
#include <iostream>

#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlType::~CqlType()
{

}


#define IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(_cls, _desc, _size, _unmarshal) \
    PyObject* _cls::Deserialize(Buffer& buffer, int)                    \
    {                                                                   \
        const unsigned char* data = buffer.Consume(_size);              \
        if (!data)                                                      \
        {                                                               \
            PyErr_SetString(PyExc_EOFError,                             \
                            "unexpected end of buffer deserializing "   \
                            _desc);                                     \
            return NULL;                                                \
        }                                                               \
                                                                        \
        return _unmarshal(data);                                        \
    }


IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(CqlInt32Type,
                                      "signed 32-bit integer",
                                      4,
                                      UnmarshalInt32)
IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(CqlLongType,
                                      "signed 64-bit integer",
                                      8,
                                      UnmarshalInt64)
IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(CqlFloatType,
                                      "32-bit floating point number",
                                      4,
                                      UnmarshalFloat32)
IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(CqlDoubleType,
                                      "64-bit floating point number",
                                      8,
                                      UnmarshalFloat64)
IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE(CqlBooleanType,
                                      "boolean",
                                      1,
                                      UnmarshalBoolean)


#undef IMPLEMENT_SIMPLE_CQL_TYPE_DESERIALIZE


PyObject* CqlBytesType::Deserialize(Buffer& buffer, int)
{
    const std::size_t size = buffer.Residual();
    if (size == 0)
        return PyString_FromStringAndSize("", 0);

    return PyString_FromStringAndSize((const char*)(buffer.Consume(size)),
                                      Py_ssize_t(size));
}

PyObject* CqlUtf8Type::Deserialize(Buffer& buffer, int)
{
    const std::size_t size = buffer.Residual();
    const char* data = (size ?
                        (const char*)(buffer.Consume(size)) :
                        "");

    ScopedReference str(PyString_FromStringAndSize(data, Py_ssize_t(size)));
    if (!str)
        return NULL;

    return PyString_AsDecodedObject(str.Get(), "utf-8", "strict");
}

PyObject* CqlUuidType::Deserialize(Buffer& buffer, int)
{
    const std::size_t size = buffer.Residual();
    const char* data = (size ?
                        (const char*)(buffer.Consume(size)) :
                        "");

    ScopedReference str(PyString_FromStringAndSize(data, Py_ssize_t(size)));
    if (!str)
        return NULL;

    ScopedReference args(PyTuple_Pack(2, Py_None, str.Get()));
    if (!args)
        return NULL;

    return PyObject_CallObject(_pythonUuidType, args.Get());
}

PyObject* CqlInetAddressType::Deserialize(Buffer& buffer, int)
{
    const std::size_t size = buffer.Residual();

    if ((size != 4) && (size != 16))
    {
        PyErr_SetString(PyExc_ValueError,
                        "expected buffer to either represent a 4 or 16 octet "
                        "network address");
        return NULL;
    }

    const char* data = (size ? (const char*)(buffer.Consume(size)) : NULL);
    char presentation[INET6_ADDRSTRLEN];

    if (!inet_ntop(size == 4 ? AF_INET : AF_INET6,
                   data,
                   presentation,
                   INET6_ADDRSTRLEN))
    {
        PyErr_SetString(PyExc_OSError, "error converting Internet address");
        return NULL;
    }

    return PyString_FromString(presentation);
}

PyObject* CqlDateType::Deserialize(Buffer& buffer, int)
{
    const unsigned char* data = buffer.Consume(8);
    if (!data)
    {
        PyErr_SetString(PyExc_EOFError,
                        "unexpected end of buffer deserializing date");
        return NULL;
    }

    int64_t timestampMs = UnpackInt64(data);

    ScopedReference pyTimestamp(PyFloat_FromDouble(double(timestampMs) /
                                                   1000.0));
    if (!pyTimestamp)
        return NULL;

    ScopedReference args(PyTuple_Pack(1, pyTimestamp.Get()));
    if (!args)
        return NULL;

    return PyObject_CallObject(_pyDatetimeDatetimeUtcFromTimestamp,
                               args.Get());
}


PyObject* CqlIntegerType::Deserialize(Buffer& buffer, int)
{
    const std::size_t size = buffer.Residual();
    return UnmarshalVarint(buffer.Consume(size), size);
}


PyObject* CqlDecimalType::Deserialize(Buffer& buffer, int)
{
    // Deserialize the scale.
    const unsigned char* scaleData = buffer.Consume(4);
    if (!scaleData)
    {
        PyErr_SetString(PyExc_EOFError,
                        "unexpected end of buffer deserializing "
                        "decimal number");
        return NULL;
    }
    int32_t negativeScale = UnpackInt32(scaleData);

    ScopedReference scale(PyInt_FromLong(-negativeScale));
    if (!scale)
        return NULL;

    // Deserialize the unscaled value.
    const std::size_t size = buffer.Residual();
    ScopedReference unscaled(UnmarshalVarint(buffer.Consume(size), size));
    if (!unscaled)
        return NULL;

    // Format the string representation of the decimal number.
    ScopedReference format(PyString_FromString("%de%d"));
    if (!format)
        return NULL;

    ScopedReference formatArgs(PyTuple_Pack(2, unscaled.Get(), scale.Get()));
    if (!formatArgs)
        return NULL;

    ScopedReference stringRepr(PyString_Format(format.Get(),
                                               formatArgs.Get()));
    if (!stringRepr)
        return NULL;

    ScopedReference args(PyTuple_Pack(1, stringRepr.Get()));
    if (!args)
        return NULL;

    return PyObject_CallObject(_pythonDecimalType, args.Get());
}

CqlProxyType::CqlProxyType(CqlType* wrappedType)
    :   CqlType(),
        _wrappedType(wrappedType)
{

}

CqlProxyType::~CqlProxyType()
{
    delete _wrappedType;
}

PyObject* CqlProxyType::Deserialize(Buffer& buffer, int protocolVersion)
{
    return _wrappedType->Deserialize(buffer, protocolVersion);
}

CqlFrozenType::CqlFrozenType(CqlType* wrappedType)
    :   CqlProxyType(wrappedType)
{

}

CqlReversedType::CqlReversedType(CqlType* wrappedType)
    :   CqlProxyType(wrappedType)
{

}

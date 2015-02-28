#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlMapType::CqlMapType(CqlType* keyType,
                       CqlType* valueType,
                       PyObject* pyMapType)
    :   _keyType(keyType),
        _valueType(valueType),
        _pyMapType(pyMapType)
{

}

CqlMapType::~CqlMapType()
{
    delete _keyType;
    delete _valueType;
}

PyObject* CqlMapType::Deserialize(Buffer& buffer, int protocolVersion)
{
    // Determine the number of items in the set.
    std::size_t itemCount;
    std::size_t sizeSize = (protocolVersion >= 3 ? 4 : 2);

    const unsigned char* itemCountData = buffer.Consume(sizeSize);
    if (!itemCountData)
    {
        PyErr_SetString(PyExc_EOFError,
                        "unexpected end of buffer while reading map "
                        "item count");
        return NULL;
    }

    itemCount = (protocolVersion >= 3 ?
                 UnpackInt32(itemCountData) :
                 UnpackInt16(itemCountData));

    // Initialize a tuple to contain the items.
    PyObject* tuple = PyTuple_New(itemCount);
    if (!tuple)
        return NULL;

    // Drain the items from the set.
    for (std::size_t i = 0; i < itemCount; ++i)
    {
        // Read the size of the key.
        const unsigned char* sizeData = buffer.Consume(sizeSize);
        const unsigned char* itemData;

        if (!sizeData)
        {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading map "
                            "key size");
            return NULL;
        }

        int32_t size = (protocolVersion >= 3 ?
                        UnpackInt32(sizeData) :
                        UnpackInt16(sizeData));

        PyObject* key;

        if (size < 0)
            key = _keyType->Empty();
        else
        {
            // Create a local buffer for the key.
            itemData = buffer.Consume(size);
            if (!itemData)
            {
                Py_DECREF(tuple);
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading map "
                                "key data");
                return NULL;
            }

            Buffer keyBuffer(itemData, size);
            key = _keyType->Deserialize(keyBuffer, protocolVersion);
            if (!key)
            {
                Py_DECREF(tuple);
                return NULL;
            }
        }

        // Read the size of the value.
        sizeData = buffer.Consume(sizeSize);
        if (!sizeData)
        {
            Py_DECREF(tuple);
            Py_DECREF(key);
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading map "
                            "value size");
            return NULL;
        }

        size = (protocolVersion >= 3 ?
                UnpackInt32(sizeData) :
                UnpackInt16(sizeData));

        PyObject* value;

        if (size < 0)
            value = _valueType->Empty();
        else
        {
            // Create a local buffer for the value.
            itemData = buffer.Consume(size);
            if (!itemData)
            {
                Py_DECREF(tuple);
                Py_DECREF(key);
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading map "
                                "value");
                return NULL;
            }

            Buffer valueBuffer(itemData, size);
            value = _valueType->Deserialize(valueBuffer, protocolVersion);
            if (!value)
            {
                Py_DECREF(tuple);
                Py_DECREF(key);
                return NULL;
            }
        }

        PyObject* pair = PyTuple_Pack(2, key, value);

        if (!pair)
        {
            Py_DECREF(key);
            Py_DECREF(value);
            Py_DECREF(tuple);
            return NULL;
        }

        PyTuple_SetItem(tuple, i, pair);
    }

    // Construct a map from the tuple.
    PyObject* map = NULL;
    PyObject* args = PyTuple_Pack(1, tuple);
    if (args)
    {
        map = PyObject_CallObject(_pyMapType, args);
        Py_DECREF(args);
    }

    return map;
}

#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "cql_type_utils.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlTupleType::CqlTupleType(const std::vector<CqlTypeReference*>& subtypes)
    :   _subtypes(subtypes)
{
    
}

CqlTupleType* CqlTupleType::FromPython(PyObject* pyCqlType,
                                       CqlTypeFactory& factory)
{
    std::vector<CqlTypeReference*> subtypes;
    if (!ResolveSubtypes(pyCqlType, factory, subtypes))
        return NULL;

    return new CqlTupleType(subtypes);
}

CqlTupleType::~CqlTupleType()
{
    std::vector<CqlTypeReference*>::iterator it = _subtypes.begin();

    while (it != _subtypes.end())
        delete *it++;
}

PyObject* CqlTupleType::Deserialize(Buffer& buffer, int protocolVersion)
{
    // Items in tuples are always encoded with at least protocol version 3.
    if (protocolVersion < 3)
        protocolVersion = 3;

    // Initialize a tuple.
    PyObject* tuple = PyTuple_New(_subtypes.size());
    if (!tuple)
        return NULL;

    // Drain as many items from the buffer as possible.
    std::size_t missing = _subtypes.size();

    for (std::size_t i = 0; i < _subtypes.size(); ++i)
    {
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(4);
        if (!sizeData)
            break;
        int32_t size = UnpackInt32(sizeData);

        // Create a local buffer for the item.
        if (size < 0)
        {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_ValueError, "negative item size in tuple");
            return NULL;
        }

        const unsigned char* itemData = buffer.Consume(size);
        if (!itemData)
        {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading tuple");
            return NULL;
        }

        Buffer itemBuffer(itemData, size);
        PyObject* des = _subtypes[i]->Get()->Deserialize(itemBuffer,
                                                         protocolVersion);
        if (!des)
        {
            Py_DECREF(tuple);
            return NULL;
        }

        PyTuple_SetItem(tuple, i, des);

        --missing;
    }

    // Backfill with Nones.
    while (missing--)
        PyTuple_SetItem(tuple, missing, Py_None);

    return tuple;
}

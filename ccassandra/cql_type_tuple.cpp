#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlTupleType::CqlTupleType(const std::vector<CqlType*>& subtypes)
    :   _subtypes(subtypes)
{
    
}

CqlTupleType::~CqlTupleType()
{
    std::vector<CqlType*>::iterator it = _subtypes.begin();

    while (it != _subtypes.end())
        delete *it++;
}

PyObject* CqlTupleType::Deserialize(Buffer& buffer, int protocolVersion)
{
    // Items in tuples are always encoded with at least protocol version 3.
    if (protocolVersion < 3)
        protocolVersion = 3;

    // Initialize a tuple.
    ScopedReference tuple(PyTuple_New(_subtypes.size()));
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
        PyObject* des;

        if (size < 0)
            des = _subtypes[i]->Empty();
        else
        {
            const unsigned char* itemData = buffer.Consume(size);
            if (!itemData)
            {
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading tuple");
                return NULL;
            }

            Buffer itemBuffer(itemData, size);
            des = _subtypes[i]->Deserialize(itemBuffer, protocolVersion);
            if (!des)
                return NULL;
        }

        PyTuple_SetItem(tuple.Get(), i, des);

        --missing;
    }

    // Backfill with Nones.
    while (missing--)
    {
        Py_INCREF(Py_None);
        PyTuple_SetItem(tuple.Get(), missing, Py_None);
    }

    return tuple.Steal();
}

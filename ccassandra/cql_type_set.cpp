#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "cql_type_utils.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlSetType::CqlSetType(CqlTypeReference* itemType)
    :   _itemType(itemType)
{
    ImportPythonModuleOrThrow(pyCassandraUtil, "cassandra.util");
    StoreAttributeOrThrow(pySetType, pyCassandraUtil, "sortedset");
    _pySetType = pySetType;
}

CqlSetType* CqlSetType::FromPython(PyObject* pyCqlType,
                                   CqlTypeFactory& factory)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlTypeReference*> subtypes;
    if (!ResolveSubtypes(pyCqlType, factory, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "set does not have one subtype");
        return NULL;
    }

    return new CqlSetType(subtypes[0]);
}

CqlSetType::~CqlSetType()
{
    delete _itemType;
}

PyObject* CqlSetType::Deserialize(Buffer& buffer, int protocolVersion)
{
    // Determine the number of items in the set.
    std::size_t itemCount;
    std::size_t sizeSize = (protocolVersion >= 3 ? 4 : 2);

    const unsigned char* itemCountData = buffer.Consume(sizeSize);
    if (!itemCountData)
    {
        PyErr_SetString(PyExc_EOFError,
                        "unexpected end of buffer while reading set");
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
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(sizeSize);
        if (!sizeData)
            break;
        int32_t size = (protocolVersion >= 3 ?
                        UnpackInt32(sizeData) :
                        UnpackInt16(sizeData));

        // Create a local buffer for the item.
        if (size < 0)
        {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_ValueError, "negative item size in set");
            return NULL;
        }

        const unsigned char* itemData = buffer.Consume(size);
        if (!itemData)
        {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading set");
            return NULL;
        }

        Buffer itemBuffer(itemData, size);
        PyObject* des = _itemType->Get()->Deserialize(itemBuffer,
                                                      protocolVersion);
        if (!des)
        {
            Py_DECREF(tuple);
            return NULL;
        }

        PyTuple_SetItem(tuple, i, des);
    }

    // Construct a set from the tuple.
    PyObject* set = NULL;
    PyObject* args = PyTuple_Pack(1, tuple);
    if (args)
    {
        set = PyObject_CallObject(_pySetType, args);
        Py_DECREF(args);
    }

    return set;
}

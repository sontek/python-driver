#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "cql_type_utils.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlListType::CqlListType(CqlTypeReference* itemType)
    :   _itemType(itemType)
{
    
}

CqlListType* CqlListType::FromPython(PyObject* pyCqlType,
                                     CqlTypeFactory& factory)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlTypeReference*> subtypes;
    if (!ResolveSubtypes(pyCqlType, factory, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "list does not have one subtype");
        return NULL;
    }

    return new CqlListType(subtypes[0]);
}

CqlListType::~CqlListType()
{
    delete _itemType;
}

PyObject* CqlListType::Deserialize(Buffer& buffer, int protocolVersion)
{
    // Determine the number of items in the list.
    std::size_t itemCount;
    std::size_t sizeSize = (protocolVersion >= 3 ? 4 : 2);

    const unsigned char* itemCountData = buffer.Consume(sizeSize);
    if (!itemCountData)
    {
        PyErr_SetString(PyExc_EOFError,
                        "unexpected end of buffer while reading list");
        return NULL;
    }

    itemCount = (protocolVersion >= 3 ?
                 UnpackInt32(itemCountData) :
                 UnpackInt16(itemCountData));

    // Initialize a list.
    PyObject* list = PyList_New(itemCount);
    if (!list)
        return NULL;

    // Drain the items from the list.
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
            Py_DECREF(list);
            PyErr_SetString(PyExc_ValueError, "negative item size in list");
            return NULL;
        }

        const unsigned char* itemData = buffer.Consume(size);
        if (!itemData)
        {
            Py_DECREF(list);
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading list");
            return NULL;
        }

        Buffer itemBuffer(itemData, size);
        PyObject* des = _itemType->Get()->Deserialize(itemBuffer,
                                                      protocolVersion);
        if (!des)
        {
            Py_DECREF(list);
            return NULL;
        }

        PyList_SetItem(list, i, des);
    }

    return list;
}

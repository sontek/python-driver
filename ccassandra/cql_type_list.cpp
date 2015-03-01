#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlListType::CqlListType(CqlType* itemType)
    :   _itemType(itemType)
{
    
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
    ScopedReference list(PyList_New(itemCount));
    if (!list)
        return NULL;

    // Drain the items from the list.
    for (std::size_t i = 0; i < itemCount; ++i)
    {
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(sizeSize);
        if (!sizeData)
        {
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading list");
            return NULL;
        }

        int32_t size = (protocolVersion >= 3 ?
                        UnpackInt32(sizeData) :
                        UnpackInt16(sizeData));

        // Create a local buffer for the item.
        PyObject* des;

        if (size < 0)
            des = _itemType->Empty();
        else
        {
            const unsigned char* itemData = buffer.Consume(size);
            if (!itemData)
            {
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading list");
                return NULL;
            }

            Buffer itemBuffer(itemData, size);
            des = _itemType->Deserialize(itemBuffer, protocolVersion);
            if (!des)
                return NULL;
        }

        PyList_SetItem(list.Get(), i, des);
    }

    return list.Steal();
}

#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlSetType::CqlSetType(CqlType* itemType, PyObject* pySetType)
    :   _itemType(itemType),
        _pySetType(pySetType)
{

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
    ScopedReference tuple(PyTuple_New(itemCount));
    if (!tuple)
        return NULL;

    // Drain the items from the set.
    for (std::size_t i = 0; i < itemCount; ++i)
    {
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(sizeSize);
        if (!sizeData)
        {
            PyErr_SetString(PyExc_EOFError,
                            "unexpected end of buffer while reading set");
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
                                "unexpected end of buffer while reading set");
                return NULL;
            }

            Buffer itemBuffer(itemData, size);
            des = _itemType->Deserialize(itemBuffer, protocolVersion);
            if (!des)
                return NULL;
        }

        PyTuple_SetItem(tuple.Get(), i, des);
    }

    // Construct a set from the tuple.
    ScopedReference args(PyTuple_Pack(1, tuple.Get()));
    if (!args)
        return NULL;

    return PyObject_CallObject(_pySetType, args.Get());
}

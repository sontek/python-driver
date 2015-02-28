#include "cql_types.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlUserType::CqlUserType(const NamesAndTypeVector& namesAndTypes,
                         PyObject* pyMappedClass,
                         PyObject* pyTupleType)
    :   _namesAndTypes(namesAndTypes),
        _pyMappedClass(pyMappedClass),
        _pyTupleType(pyTupleType)
{
    
}

CqlUserType::~CqlUserType()
{
    NamesAndTypeVector::iterator it = _namesAndTypes.begin();

    while (it != _namesAndTypes.end())
    {
        delete it->second;
        ++it;
    }

    if (_pyMappedClass)
        Py_DECREF(_pyMappedClass);
    if (_pyTupleType)
        Py_DECREF(_pyTupleType);
}

PyObject* CqlUserType::DeserializeToTuple(Buffer& buffer, int protocolVersion)
{
    // Initialize a tuple.
    PyObject* tuple = PyTuple_New(_namesAndTypes.size());
    if (!tuple)
        return NULL;
    ScopedReference tupleRef(tuple);

    // Drain as many items from the buffer as possible.
    std::size_t missing = _namesAndTypes.size();

    for (std::size_t i = 0; i < _namesAndTypes.size(); ++i)
    {
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(4);
        if (!sizeData)
            break;
        int32_t size = UnpackInt32(sizeData);

        // Create a local buffer for the item.
        PyObject* des;
        
        if (size < 0)
            des = _namesAndTypes[i].second->Empty();
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
            des = _namesAndTypes[i].second->Deserialize(itemBuffer,
                                                        protocolVersion);
            if (!des)
                return NULL;
        }

        PyTuple_SetItem(tuple, i, des);

        --missing;
    }

    // Backfill with Nones.
    while (missing--)
    {
        Py_INCREF(Py_None);
        PyTuple_SetItem(tuple, missing, Py_None);
    }

    return PyObject_CallObject(_pyTupleType, tuple);
}

PyObject* CqlUserType::DeserializeToMappedClass(Buffer& buffer, int protocolVersion)
{
    // Initialize a dict.
    PyObject* dict = PyDict_New();
    if (!dict)
        return NULL;
    ScopedReference dictRef(dict);

    // Drain as many items from the buffer as possible.
    NamesAndTypeVector::iterator it = _namesAndTypes.begin();

    while (it != _namesAndTypes.end())
    {
        // Read the size of the item.
        const unsigned char* sizeData = buffer.Consume(4);
        if (!sizeData)
            break;
        int32_t size = UnpackInt32(sizeData);

        // Create a local buffer for the item.
        PyObject* des;
        
        if (size < 0)
            des = it->second->Empty();
        else
        {
            const unsigned char* itemData = buffer.Consume(size);
            if (!itemData)
            {
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading dict");
                return NULL;
            }

            Buffer itemBuffer(itemData, size);
            des = it->second->Deserialize(itemBuffer, protocolVersion);
            if (!des)
                return NULL;
        }

        PyDict_SetItemString(dict, it->first.c_str(), des);
        ++it;
    }

    // Backfill with Nones.
    while (it != _namesAndTypes.end())
    {
        Py_INCREF(Py_None);
        PyDict_SetItemString(dict, it->first.c_str(), Py_None);
        ++it;
    }

    PyObject* emptyTuple = PyTuple_New(0);
    ScopedReference emptyTupleRef(emptyTuple);

    return PyObject_Call(_pyMappedClass, emptyTuple, dict);
}

PyObject* CqlUserType::Deserialize(Buffer& buffer, int protocolVersion)
{
    if (_pyMappedClass)
        return DeserializeToMappedClass(buffer, protocolVersion);
    return DeserializeToTuple(buffer, protocolVersion);
}

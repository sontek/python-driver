#include <stdexcept>

#include "cql_type_factory.hpp"


using namespace pyccassandra;


CqlTypeFactory::CqlTypeFactory()
{
    // Import the types on which we depend.
#define IMPORT_PYTHON_MODULE(_to, _name)            \
    PyObject* _to = PyImport_ImportModule(_name);   \
    if (!_to)                                       \
        throw std::runtime_error("error importing " _name)
#define STORE_ATTR(_to, _from, _name)                       \
    PyObject* _to = PyObject_GetAttrString(_from, _name);   \
    if (!_to) \
        throw std::runtime_error("error getting " _name)

    IMPORT_PYTHON_MODULE(pyUuid, "uuid");
    STORE_ATTR(pyUuidUuid, pyUuid, "UUID");

    IMPORT_PYTHON_MODULE(pyDatetime, "datetime");
    STORE_ATTR(pyDatetimeDatetime, pyDatetime, "datetime");

    IMPORT_PYTHON_MODULE(pyDecimal, "decimal");
    STORE_ATTR(pyDecimalDecimal, pyDecimal, "Decimal");

    // Initialize the simple type name map.
    _simpleTypeNameMap["ascii"] = new CqlAsciiType();
    _simpleTypeNameMap["boolean"] = new CqlBooleanType();
    _simpleTypeNameMap["blob"] = new CqlBytesType();
//    _simpleTypeNameMap["'org.apache.cassandra.db.marshal.ColumnToCollectionType'"] = new CqlColumnToCollectionType();
//    _simpleTypeNameMap["'org.apache.cassandra.db.marshal.CompositeType'"] = new CqlCompositeType();
    _simpleTypeNameMap["counter"] = new CqlCounterColumnType();
    _simpleTypeNameMap["timestamp"] = new CqlDateType(pyDatetimeDatetime);
    _simpleTypeNameMap["decimal"] = new CqlDecimalType(pyDecimalDecimal);
    _simpleTypeNameMap["double"] = new CqlDoubleType();
//    _simpleTypeNameMap["'org.apache.cassandra.db.marshal.DynamicCompositeType'"] = new CqlDynamicCompositeType();
    _simpleTypeNameMap["float"] = new CqlFloatType();
//    _simpleTypeNameMap["frozen"] = new CqlFrozenType();
    _simpleTypeNameMap["inet"] = new CqlInetAddressType();
    _simpleTypeNameMap["int"] = new CqlInt32Type();
    _simpleTypeNameMap["varint"] = new CqlIntegerType();
//    _simpleTypeNameMap["list"] = new CqlListType();
    _simpleTypeNameMap["bigint"] = new CqlLongType();
//    _simpleTypeNameMap["map"] = new CqlMapType();
//    _simpleTypeNameMap["'org.apache.cassandra.db.marshal.ReversedType'"] = new CqlReversedType();
//    _simpleTypeNameMap["set"] = new CqlSetType();
    _simpleTypeNameMap["timeuuid"] = new CqlUuidType(pyUuidUuid);
    _simpleTypeNameMap["timestamp"] = new CqlTimestampType(pyDatetimeDatetime);
//    _simpleTypeNameMap["tuple"] = new CqlTupleType();
    _simpleTypeNameMap["text"] = new CqlUtf8Type();
    _simpleTypeNameMap["uuid"] = new CqlUuidType(pyUuidUuid);
//    _simpleTypeNameMap["'org.apache.cassandra.db.marshal.UserType'"] = new CqlUserType();
    _simpleTypeNameMap["varchar"] = new CqlVarcharType();
}


CqlTypeReference* CqlTypeFactory::ReferenceFromPython(PyObject* pyCqlType)
{
    // Get the type name identifier from the type.
    PyObject* pyTypeName = PyObject_GetAttrString(pyCqlType, "typename");
    if (!pyTypeName)
    {
        PyErr_SetString(PyExc_TypeError, "provided type is not a CQL type");
        return NULL;
    }

    const char* cTypeName = PyString_AsString(pyTypeName);
    if (!cTypeName)
    {
        Py_DECREF(pyTypeName);
        return NULL;
    }

    std::string typeName(cTypeName);
    Py_DECREF(pyTypeName);

    // Attempt to resolve as a simple type.
    const SimpleTypeNameMap::iterator simple =
        _simpleTypeNameMap.find(typeName);

    if (simple != _simpleTypeNameMap.end())
        return new CqlBorrowedTypeReference(simple->second);

    // If we have a more complex type, identify it and attempt to construct it.
    //
    // This could possibly be sped up by using, say, a perfect hash, with
    // pointers to individual callback functions.
    if (typeName == "tuple")
    {
        CqlType* type = CqlTupleType::FromPython(pyCqlType, *this);
        return (type ? new CqlOwnedTypeReference(type) : NULL);
    }

    if (typeName == "list")
    {
        CqlType* type = CqlListType::FromPython(pyCqlType, *this);
        return (type ? new CqlOwnedTypeReference(type) : NULL);
    }

    // If not, we cannot handle this type.
    PyErr_SetString(PyExc_NotImplementedError,
                    "unsupported CQL type");
    return NULL;
}

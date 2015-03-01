#include <stdexcept>

#include "cql_type_factory.hpp"
#include "cql_type_names.hpp"


using namespace pyccassandra;


/// Import a Python module or throw an exception if not available.
#define ImportPythonModuleOrThrow(_to, _name)               \
    PyObject* _to = PyImport_ImportModule(_name);           \
    if (!_to)                                               \
        throw std::runtime_error("error importing " _name)


/// Store an attribute from a Python object or throw an exception.
#define StoreAttributeOrThrow(_to, _from, _name)            \
    PyObject* _to = PyObject_GetAttrString(_from, _name);   \
    if (!_to)                                               \
        throw std::runtime_error("error getting " _name)


/// Store an attribute from a Python object or throw an exception.
#define StoreAttributeToOrThrow(_to, _from, _name)            \
    _to = PyObject_GetAttrString(_from, _name);               \
    if (!_to)                                                 \
        throw std::runtime_error("error getting " _name)


CqlTypeFactory::CqlTypeFactory()
{
    // Import the types on which we depend.
    ImportPythonModuleOrThrow(pyUuid, "uuid");
    StoreAttributeToOrThrow(_pyUuidUuid, pyUuid, "UUID");

    ImportPythonModuleOrThrow(pyDatetime, "datetime");
    StoreAttributeOrThrow(pyDatetimeDatetime, pyDatetime, "datetime");
    StoreAttributeToOrThrow(_pyDatetimeDatetimeUtcFromTimestamp,
                            pyDatetimeDatetime,
                            "utcfromtimestamp");

    ImportPythonModuleOrThrow(pyDecimal, "decimal");
    StoreAttributeToOrThrow(_pyDecimalDecimal, pyDecimal, "Decimal");

    ImportPythonModuleOrThrow(pyCassandraUtil, "cassandra.util");
    StoreAttributeToOrThrow(_pySortedSet, pyCassandraUtil, "sortedset");
    StoreAttributeToOrThrow(_pyOrderedMap, pyCassandraUtil, "OrderedMap");

    ImportPythonModuleOrThrow(pyCassandraCqltypes, "cassandra.cqltypes");
    StoreAttributeToOrThrow(_pyCassandraCqlUserType,
                            pyCassandraCqltypes,
                            "UserType");
}


CqlType* CqlTypeFactory::FromPython(PyObject* pyCqlType)
{
    // Get the type name identifier from the type.
    ScopedReference pyTypeName(PyObject_GetAttrString(pyCqlType, "typename"));
    if (!pyTypeName)
    {
        PyErr_SetString(PyExc_TypeError, "provided type is not a CQL type");
        return NULL;
    }

    const char* cTypeName = PyString_AsString(pyTypeName.Get());
    if (!cTypeName)
        return NULL;

    CqlTypeName typeName = CqlTypeNameFromString(cTypeName);

    // Resolve depending on type name.
    switch (typeName)
    {
    case CqlUnknownTypeName:
        break;
    case CqlAsciiTypeName:
        return new CqlAsciiType();
    case CqlBooleanTypeName:
        return new CqlBooleanType();
    case CqlBlobTypeName:
        return new CqlBytesType();
    case CqlColumnToCollectionTypeName:
        break;
    case CqlCompositeTypeName:
        break;
    case CqlCounterTypeName:
        return new CqlCounterColumnType();
    case CqlDecimalTypeName:
        return new CqlDecimalType(_pyDecimalDecimal);
    case CqlDoubleTypeName:
        return new CqlDoubleType();
    case CqlDynamicCompositeTypeName:
        break;
    case CqlFloatTypeName:
        return new CqlFloatType();
    case CqlFrozenTypeName:
        return FrozenFromPython(pyCqlType);
    case CqlInetTypeName:
        return new CqlInetAddressType();
    case CqlInt32TypeName:
        return new CqlInt32Type();
    case CqlIntegerTypeName:
        return new CqlIntegerType();
    case CqlListTypeName:
        return ListFromPython(pyCqlType);
    case CqlLongTypeName:
        return new CqlLongType();
    case CqlMapTypeName:
        return MapFromPython(pyCqlType);
    case CqlReversedTypeTypeName:
        return ReversedFromPython(pyCqlType);
    case CqlSetTypeName:
        return SetFromPython(pyCqlType);
    case CqlTimeUuidTypeName:
        return new CqlUuidType(_pyUuidUuid);
    case CqlTimestampTypeName:
        return new CqlTimestampType(_pyDatetimeDatetimeUtcFromTimestamp);
    case CqlTupleTypeName:
        return TupleFromPython(pyCqlType);
    case CqlTextTypeName:
        return new CqlUtf8Type();
    case CqlUuidTypeName:
        return new CqlUuidType(_pyUuidUuid);
    case CqlUserTypeTypeName:
        break;
    case CqlVarcharTypeName:
        return new CqlVarcharType();
    }

    if (PyObject_IsSubclass(pyCqlType, _pyCassandraCqlUserType))
        return UserTypeFromPython(pyCqlType);

    // If not, we cannot handle this type.
    PyErr_Format(PyExc_NotImplementedError,
                 "unsupported CQL type: %s",
                 cTypeName);
    return NULL;
}

CqlType* CqlTypeFactory::TupleFromPython(PyObject* pyCqlType)
{
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    return new CqlTupleType(subtypes);
}

CqlType* CqlTypeFactory::ListFromPython(PyObject* pyCqlType)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "list does not have one subtype");
        return NULL;
    }

    return new CqlListType(subtypes[0]);
}

CqlType* CqlTypeFactory::MapFromPython(PyObject* pyCqlType)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    // Make sure there's only two subtypes.
    if (subtypes.size() != 2)
    {
        PyErr_SetString(PyExc_TypeError, "map does not have two subtypes");
        return NULL;
    }

    return new CqlMapType(subtypes[0], subtypes[1], _pyOrderedMap);
}

CqlType* CqlTypeFactory::SetFromPython(PyObject* pyCqlType)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "set does not have one subtype");
        return NULL;
    }

    return new CqlSetType(subtypes[0], _pySortedSet);
}

bool CqlTypeFactory::VectorizeManyFromPython(
    const std::vector<PyObject*>& pyCqlTypes,
    std::vector<CqlType*>& types
)
{
    for (std::size_t i = 0; i < pyCqlTypes.size(); ++i)
    {
        CqlType* type = FromPython(pyCqlTypes[i]);

        if (!type)
        {
            for (std::size_t j = 0; j < types.size(); ++j)
                delete types[j];
            types.clear();

            return false;
        }

        types.push_back(type);
    }

    return true;
}

bool CqlTypeFactory::VectorizePythonSubtypes(
    PyObject* pyCqlType,
    std::vector<CqlType*>& types
)
{
    // Attempt to resolve the subtypes.
    ScopedReference pySubtypeList(PyObject_GetAttrString(pyCqlType, "subtypes"));
    if (!pySubtypeList)
        return false;

    // Vectorize the subtype list.
    std::vector<PyObject*> pyCqlTypes;

    if (!VectorizePythonContainer(pySubtypeList.Get(), pyCqlTypes))
        return false;

    // Vectorize the types.
    return VectorizeManyFromPython(pyCqlTypes, types);
}

CqlType* CqlTypeFactory::FrozenFromPython(PyObject* pyCqlType)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "frozen does not have one subtype");
        return NULL;
    }

    return new CqlFrozenType(subtypes[0]);
}

CqlType* CqlTypeFactory::ReversedFromPython(PyObject* pyCqlType)
{
    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    // Make sure there's only one subtype.
    if (subtypes.size() != 1)
    {
        PyErr_SetString(PyExc_TypeError, "reversed does not have one subtype");
        return NULL;
    }

    return new CqlReversedType(subtypes[0]);
}

CqlType* CqlTypeFactory::UserTypeFromPython(PyObject* pyCqlType)
{
    // Attempt to resolved the mapped class and tuple type.
    ScopedReference pyMappedClass(PyObject_GetAttrString(pyCqlType,
                                                         "mapped_class"));

    if ((pyMappedClass) && (pyMappedClass.Get() == Py_None))
        pyMappedClass = NULL;

    ScopedReference pyTupleType(PyObject_GetAttrString(pyCqlType,
                                                       "tuple_type"));

    if ((pyTupleType) && (pyTupleType.Get() == Py_None))
        pyTupleType = NULL;

    if ((!pyMappedClass) && (!pyTupleType))
    {
        PyErr_SetString(PyExc_TypeError,
                        "provided CQL user type does not have neither a "
                        "mapped class nor a tuple type");
        return NULL;
    }

    // Attempt to resolve the field names.
    ScopedReference pyFieldNamesList(PyObject_GetAttrString(pyCqlType,
                                                            "fieldnames"));
    if (!pyFieldNamesList)
    {
        PyErr_SetString(PyExc_TypeError,
                        "provided type is not a correct CQL user type");
        return NULL;
    }

    std::vector<PyObject*> pyFieldNames;
    if (!VectorizePythonContainer(pyFieldNamesList.Get(), pyFieldNames))
        return NULL;

    std::vector<std::string> fieldNames;
    fieldNames.reserve(pyFieldNames.size());

    for (std::size_t i = 0; i < pyFieldNames.size(); ++i)
    {
        char* fieldName;
        Py_ssize_t fieldNameLength;

        if (PyString_AsStringAndSize(pyFieldNames[i],
                                     &fieldName,
                                     &fieldNameLength) == -1)
            return NULL;

        fieldNames.push_back(std::string(fieldName, fieldNameLength));
    }

    // Attempt to resolve the subtypes.
    std::vector<CqlType*> subtypes;
    if (!VectorizePythonSubtypes(pyCqlType, subtypes))
        return NULL;

    if (subtypes.size() != fieldNames.size())
    {
        for (std::size_t i = 0; i < subtypes.size(); --i)
            delete subtypes[i];

        PyErr_SetString(PyExc_TypeError,
                        "mismatch between number of field names and subtypes");
        return NULL;
    }

    // Merge the field names and types.
    std::vector<std::pair<std::string, CqlType*> > namesAndTypes;
    namesAndTypes.reserve(subtypes.size());

    for (std::size_t i = 0; i < subtypes.size(); ++i)
        namesAndTypes.push_back(std::pair<std::string, CqlType*>(fieldNames[i],
                                                                 subtypes[i]));

    return new CqlUserType(namesAndTypes,
                           pyMappedClass.Steal(),
                           pyTupleType.Steal());
}

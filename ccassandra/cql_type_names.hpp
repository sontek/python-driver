#ifndef __PYCCASSANDRA_CQLTYPENAMES
#define __PYCCASSANDRA_CQLTYPENAMES
namespace pyccassandra
{
    /// CQL type names.
    enum CqlTypeName
    {
        CqlUnknownTypeName,
        CqlAsciiTypeName,
        CqlBooleanTypeName,
        CqlBlobTypeName,
        CqlColumnToCollectionTypeName,
        CqlCompositeTypeName,
        CqlCounterTypeName,
        CqlDecimalTypeName,
        CqlDoubleTypeName,
        CqlDynamicCompositeTypeName,
        CqlFloatTypeName,
        CqlFrozenTypeName,
        CqlInetTypeName,
        CqlInt32TypeName,
        CqlIntegerTypeName,
        CqlListTypeName,
        CqlLongTypeName,
        CqlMapTypeName,
        CqlReversedTypeTypeName,
        CqlSetTypeName,
        CqlTimeUuidTypeName,
        CqlTimestampTypeName,
        CqlTupleTypeName,
        CqlTextTypeName,
        CqlUuidTypeName,
        CqlUserTypeTypeName,
        CqlVarcharTypeName
    };


    /// CQL type name from identifier string.

    /// @param string Identifier string.
    /// @returns the type name if valid or @ref CqlUnknownTypeName.
    CqlTypeName CqlTypeNameFromString(const char* string);
}
#endif

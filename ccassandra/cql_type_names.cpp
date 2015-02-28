#include "cql_type_names.hpp"


using namespace pyccassandra;


#include "cql_type_name_hash.cpp"


CqlTypeName pyccassandra::CqlTypeNameFromString(const char* string)
{
    const struct CqlTypeNameIdentification* identification =
        CqlTypeNameHash::FromString(string, strlen(string));

    return (identification ? identification->typeName : CqlUnknownTypeName);
}

#ifndef __PYCCASSANDRA_CQLTYPEUTILS
#define __PYCCASSANDRA_CQLTYPEUTILS
#include <stdexcept>

#include "cql_types.hpp"


namespace pyccassandra
{
    /// Resolve the subtypes of a CQL type.

    /// @param pyCqlType Python representation of the CQL type.
    /// @param factory CQL type factory.
    /// @param subtypes Reference to vector to hold subtypes on success.
    /// @returns true if the subtypes were successfully resolved, otherwise
    /// false, indicating failure with the corresponding Python exception set.
    bool ResolveSubtypes(PyObject* pyCqlType,
                         CqlTypeFactory& factory,
                         std::vector<CqlTypeReference*>& subtypes);


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
}
#endif

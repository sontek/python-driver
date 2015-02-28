#ifndef __PYCCASSANDRA_CQLTYPEFACTORY
#define __PYCCASSANDRA_CQLTYPEFACTORY
#include <map>
#include <string>

#include "cql_types.hpp"


namespace pyccassandra
{
    /// CQL type factory.

    /// Produceses *references* to CQL type representations. References are
    /// only guaranteed to exists during the lifetime of the type factory.
    class CqlTypeFactory
    {
    public:
        /// Initialize a CQL type factory.

        /// May throw std::runtime_error if an expected Python construct is not
        /// available.
        CqlTypeFactory();


        ~CqlTypeFactory();


        /// Get a CQL type representation reference from a Python CQL type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* FromPython(PyObject* pyCqlType);


        /// Vectorize many CQL types from a list of Python CQL types.
        bool VectorizeManyFromPython(const std::vector<PyObject*>& pyCqlTypes,
                                     std::vector<CqlType*>& types);

    private:
        CqlTypeFactory(const CqlTypeFactory&);
        CqlTypeFactory& operator =(const CqlTypeFactory&);


        PyObject* _pyUuidUuid;
        PyObject* _pyDatetimeDatetimeUtcFromTimestamp;
        PyObject* _pyDecimalDecimal;
        PyObject* _pySortedSet;
        PyObject* _pyOrderedMap;


        /// Get a CQL tuple type representation form a Python CQL tuple type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* TupleFromPython(PyObject* pyCqlType);


        /// Get a CQL list type representation form a Python CQL list type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* ListFromPython(PyObject* pyCqlType);


        /// Get a CQL map type representation form a Python CQL map type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* MapFromPython(PyObject* pyCqlType);
        

        /// Get a CQL set type representation form a Python CQL set type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* SetFromPython(PyObject* pyCqlType);


        /// Get a CQL frozen type representation form a Python CQL frozen type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL type representation if possible, otherwise NULL
        /// with the appropriate Python error set.
        CqlType* FrozenFromPython(PyObject* pyCqlType);


        /// Vectorize CQL subtypes from a Python CQL type.
        bool VectorizePythonSubtypes(PyObject* pyCqlType,
                                     std::vector<CqlType*>& types);
    };
}
#endif

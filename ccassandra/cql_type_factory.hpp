#ifndef __PYCCASSANDRA_CQLTYPEFACTORY
#define __PYCCASSANDRA_CQLTYPEFACTORY
#include <map>
#include <string>

#include "cql_types.hpp"


namespace pyccassandra
{
    class CqlTypeReference
    {
    public:
        virtual ~CqlTypeReference() {}
        CqlType* Get()
        {
            return Referenced;
        }
    protected:
        CqlTypeReference(CqlType* referenced)
            :   Referenced(referenced)
        {}

        CqlType* Referenced;
    };


    class CqlBorrowedTypeReference
        :   public CqlTypeReference
    {
    public:
        CqlBorrowedTypeReference(CqlType* referenced)
            :   CqlTypeReference(referenced)
        {}

        virtual ~CqlBorrowedTypeReference() {}
    };
    

    class CqlOwnedTypeReference
        :   public CqlTypeReference
    {
    public:
        CqlOwnedTypeReference(CqlType* referenced)
            :   CqlTypeReference(referenced)
        {}
        
        virtual ~CqlOwnedTypeReference()
        {
            delete Referenced;
        }
    };


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
        CqlTypeReference* ReferenceFromPython(PyObject* pyCqlType);
    private:
        typedef std::map<std::string, CqlType*> SimpleTypeNameMap;
        
        CqlTypeFactory(const CqlTypeFactory&);
        CqlTypeFactory& operator =(const CqlTypeFactory&);

        SimpleTypeNameMap _simpleTypeNameMap;
    };
}
#endif

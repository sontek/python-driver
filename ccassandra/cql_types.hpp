#ifndef __PYCCASSANDRA_CQLTYPES
#define __PYCCASSANDRA_CQLTYPES
#include <vector>

#include "buffer.hpp"
#include "python.hpp"


namespace pyccassandra
{
    class CqlTypeFactory;

    
    /// CQL type.
    class CqlType
    {
    public:
        CqlType() {}
        virtual ~CqlType();


        /// Deserialize the data type from a buffer.

        /// @param buffer Buffer.
        /// @param protocolVersion Protocol version.
        /// @returns a pointer to the deserialized Python object representation
        /// of the value if successful, otherwise NULL, in which case the
        /// proper Python exception has been set.
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion) = 0;
    private:
        CqlType(CqlType&);
        CqlType& operator =(CqlType&);
    };


#define DECLARE_SIMPLE_CQL_TYPE_CLASS(_cls)                        \
    class _cls                                                     \
        :   public CqlType                                         \
    {                                                              \
    public:                                                        \
        _cls()                                                     \
            :   CqlType()                                          \
        {}                                                         \
        virtual ~_cls() {}                                         \
        virtual PyObject* Deserialize(Buffer& buffer,              \
                                      int protocolVersion);        \
    }


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


    /// 32-bit signed integer CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlInt32Type);


    /// 64-bit signed integer CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlLongType);


    /// Counter column CQL type.
    typedef CqlLongType CqlCounterColumnType;


    /// 32-bit floating point CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlFloatType);


    /// 64-bit floating point CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlDoubleType);


    /// Boolean CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlBooleanType);


    /// Bytes CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlBytesType);


    /// ASCII CQL type.
    typedef CqlBytesType CqlAsciiType;


    /// UTF-8 CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlUtf8Type);


    /// Varchar CQL type.
    typedef CqlUtf8Type CqlVarcharType;


    /// Inet address CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlInetAddressType);


    /// Integer CQL type (varint.)
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlIntegerType);


    /// UUID CQL type.
    class CqlUuidType
        :   public CqlType
    {
    public:
        CqlUuidType(PyObject* pythonUuidType)
            :   CqlType(),
                _pythonUuidType(pythonUuidType)
        {}
        virtual ~CqlUuidType() {}
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        PyObject* _pythonUuidType;
    };


    /// Date CQL type.
    class CqlDateType
        :   public CqlType
    {
    public:
        CqlDateType(PyObject* pythonDatetimeType)
            :   CqlType(),
                _pythonDatetimeUtcFromTimestamp(
                    PyObject_GetAttrString(pythonDatetimeType,
                                           "utcfromtimestamp")
                )
        {}
        virtual ~CqlDateType() {}
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        PyObject* _pythonDatetimeUtcFromTimestamp;
    };

    
    /// Timestamp CQL type.
    typedef CqlDateType CqlTimestampType;


    /// Decimal CQL type.
    class CqlDecimalType
        :   public CqlType
    {
    public:
        CqlDecimalType(PyObject* pythonDecimalType)
            :   CqlType(),
                _pythonDecimalType(pythonDecimalType)
        {}
        virtual ~CqlDecimalType() {}
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        PyObject* _pythonDecimalType;
    };


    /// Tuple CQL type.
    class CqlTupleType
        :   public CqlType
    {
    public:
        /// Initialize a tuple CQL type.

        /// @param subtypes Subtypes. The tuple type takes over ownership of
        /// the references, and they should therefore *not* be released by
        /// the caller. This is not enforced, so be wary.
        CqlTupleType(const std::vector<CqlTypeReference*>& subtypes);


        /// Tuple CQL type from Python CQL type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL tuple type representation for the Python CQL type
        /// if successful, otherwise NULL, in which case appropriate Python
        /// errors have been set.
        static CqlTupleType* FromPython(PyObject* pyCqlType,
                                        CqlTypeFactory& factory);


        virtual ~CqlTupleType();
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        std::vector<CqlTypeReference*> _subtypes;
    };


    /// List CQL type.
    class CqlListType
        :   public CqlType
    {
    public:
        /// Initialize a list CQL type.

        /// @param itemType Item type. The list type takes over ownership of
        /// the reference, and it should therefore *not* be released by the
        /// caller. This is not enforced, so be wary.
        CqlListType(CqlTypeReference* itemType);


        /// List CQL type from Python CQL type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL list type representation for the Python CQL type
        /// if successful, otherwise NULL, in which case appropriate Python
        /// errors have been set.
        static CqlListType* FromPython(PyObject* pyCqlType,
                                       CqlTypeFactory& factory);


        virtual ~CqlListType();
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        CqlTypeReference* _itemType;
    };


    /// Set CQL type.
    class CqlSetType
        :   public CqlType
    {
    public:
        /// Initialize a set CQL type.

        /// @param itemType Item type. The set type takes over ownership of
        /// the reference, and it should therefore *not* be released by the
        /// caller. This is not enforced, so be wary.
        CqlSetType(CqlTypeReference* itemType);


        /// Set CQL type from Python CQL type.

        /// @param pyCqlType Python CQL type.
        /// @returns the CQL set type representation for the Python CQL type
        /// if successful, otherwise NULL, in which case appropriate Python
        /// errors have been set.
        static CqlSetType* FromPython(PyObject* pyCqlType,
                                      CqlTypeFactory& factory);


        virtual ~CqlSetType();
        virtual PyObject* Deserialize(Buffer& buffer,
                                      int protocolVersion);
    private:
        CqlTypeReference* _itemType;
        PyObject* _pySetType;
    };


#undef DECLARE_SIMPLE_CQL_TYPE_CLASS
}
#endif

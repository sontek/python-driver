#ifndef __PYCCASSANDRA_CQLRESULTROWREADER
#define __PYCCASSANDRA_CQLRESULTROWREADER
#include "cql_types.hpp"


namespace pyccassandra
{
    /// CQL result row reader.
    class CqlResultRowReader
    {
    public:
        /// Initialize a result row reader.

        /// @param columnTypes Column types. The reader takes over ownership of
        /// the references, and they should therefore *not* be released by
        /// the caller. This is not enforced, so be wary.
        CqlResultRowReader(std::vector<CqlType*>& columnTypes);


        ~CqlResultRowReader();


        /// Read all rows from a result.

        /// @param buffer Buffer containing all the raw row data.
        /// @param rowCount Number of rows.
        /// @param protocolVersion Protocol version.
        /// @returns a Python list of row tuples on success, otherwise NULL,
        /// inidicating failure, in which case the appropriate Python exception
        /// has been set.
        PyObject* ReadAll(Buffer& buffer,
                          std::size_t rowCount,
                          int protocolVersion);
    private:
        std::vector<CqlType*> _columnTypes; ///< Column types.
    };
}
#endif

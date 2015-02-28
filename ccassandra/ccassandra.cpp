#include <map>

#include "python.hpp"
#include "marshal.hpp"
#include "cql_type_factory.hpp"
#include "cql_result_row_reader.hpp"


using namespace pyccassandra;


static CqlTypeFactory* cqlTypeFactory;


static PyObject* ccassandra_deserialize_cqltype(PyObject*,
                                                PyObject* args,
                                                PyObject* kwargs)
{
    Py_ssize_t dataLength;
    const unsigned char* data;
    PyObject* pyCqlType;
    int protocolVersion = 3;

    const char* keywords[] =
    {
        "data",
        "cql_type",
        "protocol_version",
        NULL,
    };

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "s#O|i",
                                     const_cast<char**>(keywords),
                                     &data,
                                     &dataLength,
                                     &pyCqlType,
                                     &protocolVersion))
        return NULL;

    // Get a CQL type implementation.
    CqlType* type = cqlTypeFactory->FromPython(pyCqlType);

    if (!type)
        return NULL;

    // Deserialize.
    pyccassandra::Buffer buffer(data, Py_ssize_t(dataLength));
    PyObject* result = type->Deserialize(buffer, protocolVersion);
    delete type;
    return result;
}


static PyObject* ccassandra_parse_result_rows(PyObject*,
                                              PyObject* args,
                                              PyObject* kwargs)
{
    Py_ssize_t dataLength;
    const unsigned char* data;
    unsigned long long rowCount;
    PyObject* pyColumnCqlTypeContainer;
    int protocolVersion = 3;

    const char* keywords[] =
    {
        "data",
        "row_count",
        "column_types",
        "protocol_version",
        NULL,
    };

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "s#KO|i",
                                     const_cast<char**>(keywords),
                                     &data,
                                     &dataLength,
                                     &rowCount,
                                     &pyColumnCqlTypeContainer,
                                     &protocolVersion))
        return NULL;

    // Parse the column types.
    std::vector<PyObject*> pyColumnCqlTypes;
    if (!VectorizePythonContainer(pyColumnCqlTypeContainer,
                                  pyColumnCqlTypes))
        return NULL;

    std::vector<CqlType*> columnCqlTypes;
    if (!cqlTypeFactory->VectorizeManyFromPython(pyColumnCqlTypes,
                                                 columnCqlTypes))
        return NULL;

    // Create the row reader.
    CqlResultRowReader reader(columnCqlTypes);

    // Deserialize the rows.
    Buffer rowsBuffer(data, dataLength);
    return reader.ReadAll(rowsBuffer, rowCount, protocolVersion);
}


static PyMethodDef CcassandraMethods[] =
{
    {
        "deserialize_cqltype",
        (PyCFunction)ccassandra_deserialize_cqltype,
        METH_VARARGS | METH_KEYWORDS,
        "Deserialize a CQL type"
    },
    {
        "parse_result_rows",
        (PyCFunction)ccassandra_parse_result_rows,
        METH_VARARGS | METH_KEYWORDS,
        "Parse result rows"
    },
    {
        NULL,
        NULL,
        0,
        NULL
    }
};


PyMODINIT_FUNC initccassandra(void)
{
    try
    {
        cqlTypeFactory = new CqlTypeFactory();
    }
    catch (...)
    {
        PyErr_SetString(PyExc_ImportError, "error initializing ccassandra");
        return;
    }

    // Initialize the module.
    PyObject* module;

    if (!(module = Py_InitModule("ccassandra", CcassandraMethods)))
        return;
}

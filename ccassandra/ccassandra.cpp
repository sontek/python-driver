#include <map>

#include "python.hpp"
#include "marshal.hpp"
#include "cql_type_factory.hpp"


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

    if (!PyArg_ParseTupleAndKeywords(Args,
                                     kwargs,
                                     "s#O|i",
                                     const_cast<char**>(keywords),
                                     &data,
                                     &dataLength,
                                     &pyCqlType,
                                     &protocolVersion))
        return NULL;

    // Get a CQL type implementation.
    CqlTypeReference* typeRef =
        cqlTypeFactory.ReferenceFromPython(pyCqlType);

    if (!typeRef)
        return NULL;

    // Deserialize.
    pyccassandra::Buffer buffer(data, Py_ssize_t(dataLength));
    PyObject* result = (*typeRef)->Deserialize(buffer, protocolVersion);
    delete typeRef;
    return result;
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
        PyErr_SetString(PyErr_ImportError, "error initializing ccassandra");
        return;
    }

    // Initialize the module.
    PyObject* module;

    if (!(module = Py_InitModule("ccassandra", CcassandraMethods)))
        return;
}

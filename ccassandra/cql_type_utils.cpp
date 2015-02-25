#include "cql_type_utils.hpp"
#include "cql_type_factory.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


bool pyccassandra::ResolveSubtypes(PyObject* pyCqlType,
                                   CqlTypeFactory& factory,
                                   std::vector<CqlTypeReference*>& subtypes)
{
    // Attempt to resolve the subtypes.
    PyObject* pySubtypeList = PyObject_GetAttrString(pyCqlType, "subtypes");
    if (!pySubtypeList)
        return false;

    // Resolve Python representations of subtypes.
    std::vector<PyObject*> pySubtypes;

    if (PyTuple_Check(pySubtypeList))
    {
        Py_ssize_t numSubtypes = PyTuple_Size(pySubtypeList);
        pySubtypes.reserve(numSubtypes);

        for (Py_ssize_t i = 0; i < numSubtypes; ++i)
        {
            PyObject* pySubtype = PyTuple_GetItem(pySubtypeList, i);
            if (pySubtype == NULL)
            {
                Py_DECREF(pySubtypeList);
                return false;
            }

            pySubtypes.push_back(pySubtype);
        }
    }
    else if (PyList_Check(pySubtypeList))
    {
        Py_ssize_t numSubtypes = PyList_Size(pySubtypeList);
        pySubtypes.reserve(numSubtypes);

        for (Py_ssize_t i = 0; i < numSubtypes; ++i)
        {
            PyObject* pySubtype = PyList_GetItem(pySubtypeList, i);
            if (pySubtype == NULL)
            {
                Py_DECREF(pySubtypeList);
                return false;
            }

            pySubtypes.push_back(pySubtype);
        }
    }
    else
    {
        Py_DECREF(pySubtypeList);
        PyErr_SetString(PyExc_TypeError, "invalid subtypes for tuple");
        return false;
    }

    // Resolve the subtypes.
    for (std::size_t i = 0; i < pySubtypes.size(); ++i)
    {
        PyObject* pySubtype = pySubtypes[i];
        CqlTypeReference* subtype = factory.ReferenceFromPython(pySubtype);

        if (!subtype)
        {
            for (std::size_t j = 0; j < subtypes.size(); ++j)
                delete subtypes[j];
            subtypes.clear();

            Py_DECREF(pySubtypeList);

            return false;
        }

        subtypes.push_back(subtype);
    }

    Py_DECREF(pySubtypeList);

    return true;
}

#include "python.hpp"


using namespace pyccassandra;


bool pyccassandra::VectorizePythonContainer(PyObject* container,
                                            std::vector<PyObject*>& target)
{
    if (PyTuple_Check(container))
    {
        Py_ssize_t numItems = PyTuple_Size(container);
        target.reserve(numItems);

        for (Py_ssize_t i = 0; i < numItems; ++i)
        {
            PyObject* pyItem = PyTuple_GetItem(container, i);
            if (pyItem == NULL)
            {
                target.clear();
                return false;
            }

            target.push_back(pyItem);
        }

        return true;
    }
    else if (PyList_Check(container))
    {
        Py_ssize_t numItems = PyList_Size(container);
        target.reserve(numItems);

        for (Py_ssize_t i = 0; i < numItems; ++i)
        {
            PyObject* pyItem = PyList_GetItem(container, i);
            if (pyItem == NULL)
            {
                target.clear();
                return false;
            }

            target.push_back(pyItem);
        }

        return true;
    }
    else
    {
        Py_DECREF(container);
        PyErr_SetString(PyExc_TypeError, "cannot vectorize object");
        return false;
    }
}

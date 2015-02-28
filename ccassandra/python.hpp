#ifndef __PYCCASSANDRA_PYTHON
#define __PYCCASSANDRA_PYTHON
#include <vector>

#define PY_SSIZE_T_CLEAN
#include <Python.h>


namespace pyccassandra
{
    /// Vectorize Python container.

    /// Fills the items in a Python container into a target vector.
    ///
    /// @param container Container.
    /// @param target Target vector to contain the items in the container.
    /// @returns true if vectorization succeeded, otherwise false, indicating
    /// failure with the corresponding Python exception set.
    bool VectorizePythonContainer(PyObject* container,
                                  std::vector<PyObject*>& target);
}
#endif

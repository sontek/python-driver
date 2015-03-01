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


    /// Scoped Python reference.

    /// RAII-style Python reference, which will be released upon exiting the
    /// scope. This behavior can be cancelled by calling Steal on the scoped
    /// reference.
    class ScopedReference
    {
    public:
        ScopedReference(PyObject* obj = NULL)
            :   _obj(obj)
        {
        }


        void operator =(PyObject* obj)
        {
            if (_obj)
                Py_DECREF(_obj);
            _obj = obj;
        }


        ~ScopedReference()
        {
            if (_obj)
                Py_DECREF(_obj);
        }


        /// Object.
        inline PyObject* Get() const
        {
            return _obj;
        }


        /// Steal the reference.
        PyObject* Steal()
        {
            PyObject* obj = _obj;
            _obj = NULL;
            return obj;
        }


        operator bool() const
        {
            return _obj;
        }
    private:
        PyObject* _obj;
    };
}
#endif

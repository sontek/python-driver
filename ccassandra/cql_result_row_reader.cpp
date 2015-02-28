#include "cql_result_row_reader.hpp"
#include "marshal.hpp"


using namespace pyccassandra;


CqlResultRowReader::CqlResultRowReader(std::vector<CqlType*>& columnTypes)
    :   _columnTypes(columnTypes)
{
    columnTypes.clear();
}

CqlResultRowReader::~CqlResultRowReader()
{
    std::size_t i = _columnTypes.size();
    while (i--)
        delete _columnTypes[i];
}

PyObject* CqlResultRowReader::ReadAll(Buffer& buffer,
                                      std::size_t rowCount,
                                      int protocolVersion)
{
    // Initialize the row list.
    PyObject* rows = PyList_New(Py_ssize_t(rowCount));
    if (!rows)
        return NULL;

    for (std::size_t i = 0; i < rowCount; ++i)
    {
        PyObject* row = PyTuple_New(_columnTypes.size());

        for (std::size_t j = 0; j < _columnTypes.size(); ++j)
        {
            // Read the size of the item.
            const unsigned char* sizeData = buffer.Consume(4);
            if (!sizeData)
            {
                Py_DECREF(row);
                Py_DECREF(rows);
                PyErr_SetString(PyExc_EOFError,
                                "unexpected end of buffer while reading row");
                return NULL;
            }
            int32_t size = UnpackInt32(sizeData);

            // Create a local buffer for the item.
            PyObject* des;

            if (size < 0)
                des = _columnTypes[j]->Empty();
            else
            {
                const unsigned char* itemData = buffer.Consume(size);
                if (!itemData)
                {
                    Py_DECREF(row);
                    Py_DECREF(rows);
                    PyErr_SetString(PyExc_EOFError,
                                    "unexpected end of buffer while "
                                    "reading row");
                    return NULL;
                }

                Buffer itemBuffer(itemData, size);
                des = _columnTypes[j]->Deserialize(itemBuffer,
                                                   protocolVersion);
            }

            if (!des)
            {
                Py_DECREF(row);
                Py_DECREF(rows);
                return NULL;
            }

            PyTuple_SetItem(row, j, des);
        }

        PyList_SetItem(rows, i, row);
    }

    return rows;
}

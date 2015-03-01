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
    ScopedReference rows(PyList_New(Py_ssize_t(rowCount)));
    if (!rows)
        return NULL;

    for (std::size_t i = 0; i < rowCount; ++i)
    {
        ScopedReference row(PyTuple_New(_columnTypes.size()));
        if (!row)
            return NULL;

        for (std::size_t j = 0; j < _columnTypes.size(); ++j)
        {
            // Read the size of the item.
            const unsigned char* sizeData = buffer.Consume(4);
            if (!sizeData)
            {
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
                    PyErr_SetString(PyExc_EOFError,
                                    "unexpected end of buffer while "
                                    "reading row");
                    return NULL;
                }

                Buffer itemBuffer(itemData, size);
                if (!(des = _columnTypes[j]->Deserialize(itemBuffer,
                                                         protocolVersion)))
                    return NULL;
            }

            PyTuple_SetItem(row.Get(), j, des);
        }

        PyList_SetItem(rows.Get(), i, row.Steal());
    }

    return rows.Steal();
}

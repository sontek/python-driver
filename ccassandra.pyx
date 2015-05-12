cdef native_row_parser(
     int row_count, list column_metadata, bytes f, int protocol_version,
     list coltypes
):
    print("HELLO!")

a = """
    from cassandra.protocol import read_value

    column_length = len(column_metadata)
    rows = []
    print(f)
#    for i in range(row_count):
#        print(f)
#        row = [read_value(f) for _ in range(column_length)]

#        rows.append(tuple(
#            ctype.from_binary(val, protocol_version)
#            for ctype, val in zip(coltypes, row)
#        ))

#    return rows
"""

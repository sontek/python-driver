def native_row_parser(
     row_count, column_metadata, f, protocol_version, coltypes
):
    from cassandra.protocol import read_value

    column_length = len(column_metadata)
    rows = []
    for i in range(row_count):
        row = [read_value(f) for _ in range(column_length)]

        rows.append(tuple(
            ctype.from_binary(val, protocol_version)
            for ctype, val in zip(coltypes, row)
        ))

    return rows

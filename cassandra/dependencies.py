# Add the expected build output directory for testing during development.
import sys
import sysconfig
import os
import logging

import warnings


log = logging.getLogger(__name__)

build_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'build',
    'lib.{platform}-{version[0]}.{version[1]}'.format(
        platform=sysconfig.get_platform(),
        version=sys.version_info
    )
)

if os.path.exists(build_path):
    sys.path.append(build_path)


try:
    import ccassandra

    def native_row_parser(rowcount, f, protocol_version, coltypes):
        return ccassandra.parse_result_rows(data=f.read(),
                                            column_types=coltypes,
                                            row_count=rowcount,
                                            protocol_version=protocol_version)
except ImportError:
    warnings.warn("Using pure python deserialization")

    def python_row_parser(rowcount, f, protocol_version, coltypes):
        from cassandra.protocol import read_value
        colcount = len(coltypes)
        rows = [[read_value(f) for _ in range(colcount)]
                for _ in range(rowcount)]
        return [
            tuple(ctype.from_binary(val, protocol_version)
                  for ctype, val in zip(coltypes, row))
            for row in rows]

# Add the expected build output directory for testing during development.
import sys
import sysconfig
import os


build_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)
    )))),
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
except ImportError:
    ccassandra = None

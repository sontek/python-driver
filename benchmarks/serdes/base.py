# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cProfile import Profile
from datetime import datetime
import io
import logging
import os.path
import sys
import sysconfig
from threading import Thread
import time
from optparse import OptionParser

from greplin import scales


dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..', '..'))

import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import HostDistance
from cassandra.protocol import ResultMessage
from cassandra.concurrent import execute_concurrent_with_args





log = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

have_libev = False

KEYSPACE = "testkeyspace" + str(int(time.time()))
TABLE = "testtable"


def setup(hosts):
    log.info("Using 'cassandra' package from %s", cassandra.__path__)

    cluster = Cluster(hosts)
    cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
    try:
        session = cluster.connect()

        log.debug("Creating keyspace...")
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
            """ % KEYSPACE)

        log.debug("Setting keyspace...")
        session.set_keyspace(KEYSPACE)

        log.debug("Creating table...")
        session.execute("""
            CREATE TABLE %s (
                thekey text,
                col1 text,
                col3 set<text>,
                col4 list<varint>,
                col5 list<float>,
                PRIMARY KEY ((thekey), col1)
            )
            """ % TABLE)
    finally:
        cluster.shutdown()


def teardown(hosts):
    cluster = Cluster(hosts)
    cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
    session = cluster.connect()
    session.execute("DROP KEYSPACE " + KEYSPACE)
    cluster.shutdown()


def run_threaded(thread_count, thread_class, session, query, values,
                 per_thread, cluster, profile):
    threads = []
    for i in range(thread_count):
        thread = thread_class(
            i, session, query, values, per_thread,
            cluster.protocol_version, profile)
        thread.daemon = True
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        while thread.is_alive():
            thread.join(timeout=0.5)


def benchmark(thread_class):
    options, args = parse_options()

    setup(options.hosts)

    kwargs = {'metrics_enabled': options.enable_metrics}
    if options.protocol_version:
        kwargs['protocol_version'] = options.protocol_version
    cluster = Cluster(options.hosts, **kwargs)
    session = cluster.connect(KEYSPACE)

    log.debug("Sleeping for two seconds...")
    time.sleep(2.0)

    query = session.prepare("""
        INSERT INTO {table} (thekey, col1, col3, col4, col5)
                VALUES (?, ?, ?, ?, ?)
        """.format(table=TABLE))
    values = [('key', str(i),
              {'fish', 'dog', 'cat'},
              [9999999999, 1, -999999999999],
              [999.9+f for f in range(10)])
              for i in range(options.num_rows)]

    # insert test data
    execute_concurrent_with_args(session, query, values)

    per_thread = options.num_ops // options.threads

    query = session.prepare("""
        SELECT * FROM {table} WHERE thekey = ?
        """.format(table=TABLE))
    values = ['key']

    # Wild hack to pull out the wire data
    orig_recv_results_rows = ResultMessage.recv_results_rows

    # TODO: Learn python scope rules
    recv_results_args = []
    row_data = []

    @staticmethod
    def faked_recv_results_rows(f, *args, **kwargs):
        row_data.append(f.read())
        f = io.BytesIO(row_data[0])
        recv_results_args.extend((args, kwargs))
        return orig_recv_results_rows(f, *args, **kwargs)

    ResultMessage.recv_results_rows = faked_recv_results_rows

    session.execute(query, values)

    ResultMessage.recv_results_rows = orig_recv_results_rows

    assert len(row_data[0]) != 0

    log.warn("Beginning deserialization...")
    threads = []
    start = time.time()

    for i in range(options.threads):
        thread = thread_class(
            i, per_thread, row_data[0], recv_results_args[0], recv_results_args[1],
            options.profile)
        thread.daemon = True
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        while thread.is_alive():
            thread.join(timeout=0.5)

    end = time.time()

    total = end - start

    log.info("Total time: %0.2fs" % total)
    log.info("Average throughput (for %s rows): %0.2f/sec" % (options.num_rows, options.num_ops / total))
    log.info("Average deserialization time: %0.2fms per %s row" % ((total / options.num_ops) * 1000, options.num_rows))

    cluster.shutdown()
    teardown(options.hosts)


def parse_options():
    parser = OptionParser()
    parser.add_option('-H', '--hosts', default='127.0.0.1',
                      help='cassandra hosts to connect to (comma-separated list) [default: %default]')
    parser.add_option('-t', '--threads', type='int', default=1,
                      help='number of threads [default: %default]')
    parser.add_option('-m', '--metrics', action='store_true', dest='enable_metrics',
                      help='enable and print metrics for operations')
    parser.add_option('-r', '--num-rows', type='int', default=1000,
                      help='number of rows [default: %default]')
    parser.add_option('-n', '--num-ops', type='int', default=100,
                      help='number of operations [default: %default]')
    parser.add_option('-l', '--log-level', default='info',
                      help='logging level: debug, info, warning, or error')
    parser.add_option('-p', '--profile', action='store_true', dest='profile',
                      help='Profile the run')
    parser.add_option('--protocol-version', type='int', dest='protocol_version',
                      help='Native protocol version to use')

    options, args = parser.parse_args()

    options.hosts = options.hosts.split(',')

    log.setLevel(options.log_level.upper())

    return options, args


class BenchmarkThread(Thread):
    def __init__(self, thread_num, num_ops, row_data,
                 recv_rows_args, recv_rows_kwargs, profile,):
        Thread.__init__(self)
        self.thread_num = thread_num
        self.num_ops = num_ops
        self.row_data = row_data
        self.profiler = Profile() if profile else None
        self.recv_rows_args = recv_rows_args
        self.recv_rows_kwargs = recv_rows_kwargs

    def start_profile(self):
        if self.profiler:
            self.profiler.enable()

    def finish_profile(self):
        if self.profiler:
            self.profiler.disable()
            self.profiler.dump_stats('profile-%d' % self.thread_num)

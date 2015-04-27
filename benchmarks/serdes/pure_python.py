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
import io

from base import benchmark, BenchmarkThread
from cassandra.protocol import ResultMessage


class Runner(BenchmarkThread):

    def run(self):
        self.start_profile()

        for _ in xrange(self.num_ops):
            f = io.BytesIO(self.row_data)
            ResultMessage.recv_results_rows(f, *self.recv_rows_args,
                                               **self.recv_rows_kwargs)

            # print res
        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)

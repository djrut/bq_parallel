# Copyright 2018 Google

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Configure and execute a Trial consisting of a single query run sequentially
a number (samples) of times """

import time
import numpy as np
from google.cloud import bigquery

COST_PER_MB = 5 / (1024**2)

class Trial(object):
    def __init__(self,
                 query,
                 options,
                 samples=3,
                 reducer=lambda x:np.average(x)):

        self._query = query
        self._options = options
        self._samples = samples
        self._reducer = reducer
        self.reset()

        # Create an instance of BQ client for each experiment to avoid the 10
        # thread request lib limit
        self._bq_client = bigquery.Client()

    def reset(self):
        self._response_times = []
        self._mbytes_processed = []
        self._mbytes_billed = []
        self._slot_millis = []

    def run(self):
        for sample in range(self._samples):
            start = time.time()

            query_job = self._query.execute(self._bq_client)

            _ = query_job.result()
            end = time.time()

            # Generate stats
            self._response_times.append(end - start)
            self._mbytes_processed.append(query_job.total_bytes_processed / 1024 / 1024)
            self._mbytes_billed.append(query_job.total_bytes_billed /1024 / 1024)
            self._slot_millis.append(query_job.slot_millis)

        self._output()

        return self._response_times, self._slot_millis


    def _output(self):
        print(('---- Query: "{}", mean = {:0.2f}s, min/max = {:0.2f}/{:0.2f}s, '
               'std = {:0.3f}, '
               'slot time = {:,.2f}ms, '
               'MB processed = {:,.2f}, '
               'MB billed = {:,.2f}, '
               'cost = ${:0.5f}')
              .format(self._query.name,
                      self._reducer(self._response_times),
                      np.min(self._response_times),
                      np.max(self._response_times),
                      np.std(self._response_times),
                      self._reducer(self._slot_millis),
                      self._reducer(self._mbytes_processed),
                      self._reducer(self._mbytes_billed),
                      self._reducer(self._mbytes_billed) * COST_PER_MB))

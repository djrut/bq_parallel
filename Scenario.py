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

""" Configure and execute a Scenario, which consists of one or more concurrent
Trials that are launched in parallel """

import numpy as np
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import choice
from Trial import Trial

class Scenario(object):
    def __init__(self,
                 threads,
                 samples,
                 queries,
                 options):

        self._threads = threads
        self._samples = samples
        self._queries = queries
        self._options = options

    def __repr__(self):
        return ' '.join(['Scenario(threads = {},',
                         'num. queries = {},',
                         'samples = {})']).format(self._threads,
                                                  len(self._queries),
                                                  self._samples)
    def run(self):
        print(('--- Starting scenario with threads = {}, '
               'samples = {}, use_query_cache = {}')
              .format(self._threads,
                      self._samples,
                      self._options['use_cache']))

        # TODO (djrut): When multiple queries are passed, calculate weighting for
        # each query compared to total, and divide number of threads by this ratio
        # to allow complex splits of concurrent queries to be run
        # weight_sum = sum([ query['weight'] for query in queries ])

        response_results = {}
        slot_results = {}

        with ThreadPoolExecutor(max_workers=self._threads) as executor:
            future_to_query = {}
            for _ in range(self._threads):
                # Interim solution for multiple queries
                # TODO (djrut): Implement query weighting
                query = choice(self._queries)

                trial = Trial(query=query,
                              options=self._options,
                              samples=self._samples)

                # Construct a dict of Future -> query name
                future_to_query[executor.submit(trial.run)] = query.name

            for future in as_completed(future_to_query):
                query_name = future_to_query[future]
                try:
                    response_times, slot_millis = future.result()
                except Exception as e:
                    print('Query "{}" generated exception: {}'
                              .format(query_name, repr(e)))
                else:
                    if query_name in response_results:
                        response_results[query_name].append(response_times)
                    else:
                        response_results[query_name] = [response_times]

        # TODO (djrut): Add support to export results as json/csv
        for query_name, response_times in response_results.items():
            print(('--- Query "{}", threads = {}, samples = {}, mean = {:0.2f}s, '
                   'min = {:0.2f}s, max = {:0.2f}s, '
                   'std. deviation = {:0.3f}')
                  .format(query_name,
                          self._threads,
                          len(list(itertools.chain.from_iterable(response_times))),
                          np.average(response_times),
                          np.min(response_times),
                          np.max(response_times),
                          np.std(response_times)))

        return response_results

    @property
    def threads(self):
        return self._threads

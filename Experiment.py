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

""" Class to construct and run a parallel query experiment """

import csv
import numpy as np
import itertools
from Scenario import Scenario
from Statistic import Statistic
from stats_config import stats_cfg

class Experiment(object):
    def __init__(self,
                 queries,
                 options,
                 scenario_plan,
                 samples=3):

        self._queries = queries
        self._samples = samples
        self._options = options
        self._scenario_plan = scenario_plan

        self._scenarios = []
        self._results = []
        self._stats = []

        # TODO (djrut): Implement more sophisticated scenario plans, with
        # idioms such as: 'range(x..y) step z'
        for scenario in scenario_plan.split(','):
            self._scenarios.append(Scenario(threads=int(scenario),
                                            samples=self._samples,
                                            queries=self._queries,
                                            options=self._options))

        for stat_name, stat_function in stats_cfg.items():
            self._stats.append(Statistic(name=stat_name,
                                         function=stat_function))

    def __repr__(self):
        return 'Experiment(queries = {}, options = {}, scenarios = {})'.format(self._queries,
                                                                               self._options,
                                                                               self._scenario_plan)

    def run(self,
            output_file):

        """ Execute an experiment """

        with open(output_file, 'w+', newline='') as output_file:
            if self._options['format'] == 'csv':
                results_writer = csv.writer(output_file, delimiter=',')

                # Write header row
                header_row = ['query_name','num_threads','num_samples']

                # TODO (djrut): Allow user to specify the set and sequence of
                # stats to output
                for stat in self._stats:
                    header_row.append(stat.name)

                results_writer.writerow(header_row)

            print(('- Starting experiment with scenarios = {}, '
                   'samples = {}, use_query_cache = {}')
                  .format(self._scenario_plan,
                          self._samples,
                          self._options['use_cache']))


            for scenario in self._scenarios:
                print('-- Scenario = {} concurrent threads'
                      .format(scenario.threads))

                scenario_result = scenario.run()

                self._results.append(scenario_result)

                for query_name, response_times in scenario_result.items():
                    row = [query_name,
                           len(response_times),
                           len(list(itertools
                                    .chain
                                    .from_iterable(response_times)))]

                    for stat in self._stats:
                        row.append(stat.execute(response_times))

                    results_writer.writerow(row)

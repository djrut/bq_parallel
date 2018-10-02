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

import argparse
import warnings
from QuerySet import QuerySet
from Experiment import Experiment

QUERIES = [{"name": "Default",
            "weight": 1,
            "sql": ('SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
                    'WHERE state = "TX" '
                    'LIMIT 100')}]

if __name__ == '__main__':
    # Disable the super annoying warnings that are unnecessarily generated when
    # using user credentials rather than a service account.
    # Reference: https://github.com/GoogleCloudPlatform/google-auth-library-python/issues/271

    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

    parser = argparse.ArgumentParser(description=('Utility to launch parallel BQ queries '
                                                  'and return average response time'))
    parser.add_argument(
        '--query-file',
        '-f',
        type=str,
        default=argparse.SUPPRESS,
        help='JSON file containing queries to execute')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        '--scenarios',
        type=str,
        default=argparse.SUPPRESS,
        help=('Comma separated list of concurrency scenarios to run '
              'i.e. 10,20,30,40'))

    group.add_argument(
        '--threads',
        type=int,
        default=argparse.SUPPRESS,
        help='Number of concurrent threads/queries (ad-hoc usage)')

    parser.add_argument(
        '--samples',
        type=int,
        default=3,
        action='store',
        help='Number of samples to take for each thread')

    parser.add_argument(
        '--output-file',
        '-O',
        type=str,
        required=True,
        action='store',
        help='Filename to output results to')

    parser.add_argument(
        '--format',
        type=str,
        choices=['csv'],
        default='csv',
        action='store',
        help='Final results format')

    parser.add_argument(
        '-C',
        '--use-cache',
        action='store_true',
        default=False,
        help='Enable query caching')

    args = parser.parse_args().__dict__

    # TODO (djrut): Implement global options and query specific options, such
    # that query specific options overide global options
    options = {}
    options['use_cache'] = args['use_cache']
    options['format'] = args['format']

    if args.get('query_file', None):
       queries = QuerySet.from_file(query_file=args['query_file'],
                                    options=options)
    else:
       queries = QuerySet(name='Default',
                          queries=QUERIES,
                          options=options)

    for query in queries:
        print(query)

    # Configure the experiment depending on whether the '--threads' or
    # '--scenarios' option is specified. The former is simply configured as an
    # Experiment with a single Scenario.
    if args.get('scenarios', None):
        experiment = Experiment(queries=queries,
                                options=options,
                                samples=args['samples'],
                                scenario_plan=args['scenarios'])
    else:
        experiment = Experiment(queries=queries,
                                options=options,
                                samples=args['samples'],
                                scenario_plan=str(args['threads']))

    results = experiment.run(output_file=args['output_file'])

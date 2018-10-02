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

""" Wrapper class for an iterable set of queries """

import os
import json
import yaml
from Query import Query

class QuerySet(object):
    def __init__(self,
                 name,
                 queries,
                 options):

        self._queries = []
        self._name = name

        for query in queries:
            # Handle case where options is either not set, or is empty
            query_options = query.get('options', {})
            if query_options == None:
                query_options = {}

            self._queries.append(Query(name=query['name'],
                                       options={**query_options, **options},
                                       sql=query['sql']))

    def __repr__(self):
        return 'QuerySet(name = {}, num. queries = {})'.format(self._name,
                                                              len(self._queries))
    def __len__(self):
        return len(self._queries)

    def __getitem__(self, position):
        return self._queries[position]

    @classmethod
    def from_file(self,
                  query_file,
                  options):
        _, extension = os.path.splitext(query_file)
        print(extension)

        try:
            with open(query_file) as f:
                if extension == '.json':
                    query_cfg = json.load(f)
                elif extension in ['.yaml', '.yml']:
                    query_cfg = yaml.load(f)
                    print(query_cfg)
        except Exception as e:
            print('Unable to open query file {}: {}'
                  .format(query_file, repr(e)))
            raise
        else:
            queries = [query for query in query_cfg['queries']] # LOL

            return self(name=query_cfg['name'],
                        queries=queries,
                        options=options)


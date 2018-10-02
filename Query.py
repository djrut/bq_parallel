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

""" Query wrapper class """

from google.cloud import bigquery

class Query(object):
    def __init__(self,
                 name,
                 options,
                 sql):

        self._name = name
        self._options = options
        self._sql = sql

    def __repr__(self):
        return 'Query(name = {}, options = {}, sql = {})'.format(self._name,
                                                                 self._options,
                                                                 self._sql)

    def execute(self, bq_client):
        # BQ client behavior now dictates a unique QueryJobConfig() per query
        job_config = bigquery.QueryJobConfig()

        job_config.use_query_cache = self._options['use_cache']

        query_job = bq_client.query(self._sql, job_config)
        return query_job

    @property
    def options(self):
        return self._options

    @property
    def name(self):
        return self._name

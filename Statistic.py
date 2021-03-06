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

""" Wrapper class for a statistic function that is executed over a list of
values """

class Statistic(object):
    def __init__(self,
                 name,
                 function):

        self._name = name
        self._function = function

    def execute(self,
                values):
        return self._function(values)

    @property
    def name(self):
        return self._name

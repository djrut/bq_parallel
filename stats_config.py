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

import numpy as np

def std_trim(data,
             m=3):
    """ Trim outliers using mean/std deviation method """
    return data[abs(data - np.mean(data)) < m * np.std(data)]

def median_trim(data,
                m =3.):
    """ Trim outliers using scaled median distance method """
    distance = np.abs(data - np.median(data))
    mdev = np.median(distance)
    scaled = distance/(mdev if mdev else 1.)
    return data[scaled<m]

stats_cfg = {
    "mean": lambda x: np.average(x),
    "std_dev": lambda x: np.std(x),
    "min": lambda x: np.min(x),
    "min_median_trim": lambda x: np.min(median_trim(np.array(x))),
    "min_std_trim": lambda x: np.min(std_trim(np.array(x))),
    "max": lambda x: np.max(x),
    "max_median_trim": lambda x: np.max(median_trim(np.array(x))),
    "max_std_trim": lambda x: np.max(std_trim(np.array(x))),
    "95th_percentile": lambda x: np.percentile(x, 95),
    "5th_percentile": lambda x: np.percentile(x, 5)
}

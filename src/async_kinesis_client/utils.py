# Next code block is essentially a slightly modified code from https://github.com/NerdWalletOSS/kinesis-python
# and licensed under Apache 2.0 license.
#
# Copyright 2017 NerdWallet
#
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

import sys
from collections import Iterable, Mapping


def _sizeof(obj, seen=None):
    """Recursively and fully calculate the size of an object"""
    obj_id = id(obj)
    try:
        if obj_id in seen:
            return 0
    except TypeError:
        seen = set()

    seen.add(obj_id)

    size = sys.getsizeof(obj)

    # since strings are iterables we return their size explicitly first
    if isinstance(obj, str):
        return size
    elif isinstance(obj, Mapping):
        return size + sum(
            _sizeof(key, seen) + _sizeof(val, seen)
            for key, val in obj.items()
        )
    elif isinstance(obj, Iterable):
        return size + sum(
            _sizeof(item, seen)
            for item in obj
        )

    return size

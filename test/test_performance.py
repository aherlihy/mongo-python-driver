# Copyright 2009-2015 MongoDB, Inc.
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

"""Tests for the performance spec."""

import os
import sys

sys.path[0:0] = [""]

from bson.json_util import loads
from bson import BSON, CodecOptions
from pymongo.monotonic import time
from test import unittest

NUM_ITERATIONS = 10000
MAX_ITERATION_TIME = 10

TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('performance_testdata'))
print "TEST_PATH", TEST_PATH

class Timer:
    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, *args):
        self.end = time()
        self.interval = self.end - self.start

class PerformanceTest():
    def percentile(self, percentile):
        sorted_results = sorted(self.results[:self.max_iterations])
        percentile_index = int(self.max_iterations * percentile / 100) - 1
        return sorted_results[percentile_index]

    def runTest(self):
        results = [0 for _ in range(NUM_ITERATIONS)]
        start = time()
        self.max_iterations = NUM_ITERATIONS
        for i in range(NUM_ITERATIONS):
            self.before()
            with Timer() as t:
                self.do_task()
            self.after()
            results[i] = t.interval
            if time() - start > MAX_ITERATION_TIME:
                self.max_iterations = i
                break

        self.results = results


class Featherweight(PerformanceTest):
    def setUp(self):
        # Location of JSON test specifications.
        with open(os.path.join(
                TEST_PATH, os.path.join('featherweight', self.dataset))) as f:
            self.documents = loads(f.read())

    def do_task(self):
        for doc in self.documents:
            BSON.encode(doc).decode(codec_options=CodecOptions(tz_aware=True))

    def tearDown(self):
        pass

    def before(self):
        pass

    def after(self):
        pass

class CommonFlatBSON(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'flat_bson.json'
        super(CommonFlatBSON, self).setUp()

class CommonNestedBSON(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'deep_bson.json'
        super(CommonNestedBSON, self).setUp()

class AllBSONTypes(Featherweight, unittest.TestCase):
    def setUp(self):
        self.dataset = 'full_bson.json'
        super(AllBSONTypes, self).setUp()

class Lightweight(PerformanceTest):
    def setUp(self):


if __name__ == "__main__":
    unittest.main()

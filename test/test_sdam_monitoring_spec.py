# Copyright 2015 MongoDB, Inc.
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

"""Run the sdam monitoring spec tests."""

import json
import os
import re
import sys
import time
import threading

sys.path[0:0] = [""]

from bson.json_util import object_hook
from pymongo.ismaster import IsMaster
from pymongo import monitoring
from pymongo.server_description import ServerDescription
from test import unittest, client_context
from test.utils import (single_client,
                        AllEventListener)

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'sdam_monitoring')

def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


class TestAllScenarios(unittest.TestCase):

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        cls.all_listener = AllEventListener()
        cls.saved_listeners = monitoring._LISTENERS
        monitoring._LISTENERS = monitoring._Listeners([], [], [], [])

    @classmethod
    def tearDownClass(cls):
        monitoring._LISTENERS = cls.saved_listeners



def create_test(scenario_def):
    def run_scenario(self):
        print "Running test", scenario_def['description']

        class MockMonitor(object):
            def __init__(self, server_description, topology, pool, topology_settings):
                self._server_description = server_description
                self._topology = topology

            def open(self): # TODO: mock isMaster response, then call top.on_change with fake ServerDescription
                response = scenario_def['phases'][0]['responses'][0][1]
                isMaster = IsMaster(response)
                self._server_description = ServerDescription(
                    address=self._server_description.address,
                    ismaster=isMaster)
                self._topology.on_change(self._server_description)

            def request_check(self):
                pass

            def close(self):
                pass


        m = single_client(h=scenario_def['uri'],
                          event_listeners=(self.all_listener,),
                          _monitor_class=MockMonitor)
        if not self.all_listener.results:
            print "NO RESULTS"
        for result in self.all_listener.results:
            print result.__class__.__name__
            # print result
            print "\t%s" % dict((name, getattr(result, name)) for name in dir(result) if not name.startswith('_'))
    return run_scenario


def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(
                    scenario_stream, object_hook=object_hook)
            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s' % (os.path.splitext(filename)[0],)
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()

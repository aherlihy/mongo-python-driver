# Copyright 2016 MongoDB, Inc.
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
import sys
import weakref

sys.path[0:0] = [""]

from bson.json_util import object_hook
from pymongo import common
from pymongo import monitoring
from pymongo import periodic_executor
from pymongo.ismaster import IsMaster
from pymongo.monitor import Monitor
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.server_type import SERVER_TYPE
from pymongo.topology import TOPOLOGY_TYPE
from test import unittest, client_context
from test.utils import AllEventListener, single_client, wait_until

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'sdam_monitoring')

def pretty_print(to_print, indent=0):
    old_ws = "\t" + " " * indent
    print(old_ws + "{")
    indent+=2
    ws = "\t" + " " * indent

    for key in sorted(to_print.keys()):
        if key in ("passives", "primary", "setName", "arbiters"):
            continue
        if isinstance(to_print[key], dict):
            print(ws + "%s: " % key)
            pretty_print(to_print[key], indent=indent+2)

        elif isinstance(to_print[key], list):
            if not to_print[key] or not isinstance(to_print[key][0], dict):
                print(ws + "%s: %s" % (key, str(to_print[key])))
            else:
                print(ws + "%s: [" % key)
                for item in to_print[key]:
                    pretty_print(item, indent=indent+2)
                    if item == to_print[key][-1]:
                        print(ws + "],")
        else:
            print(ws + "%s: %s," % (key, to_print[key]))

        if key == sorted(to_print.keys())[-1]:
            print(old_ws + "}")

def print_server_desc(actual, expected):
    actual_dict = {"address": "%s:%s" % actual.server_address,
                   "previousDescription":
                        {"address": "%s:%s" % actual.previous_description.address,
                        "all_hosts": actual.previous_description.all_hosts,
                        "server_type": actual.previous_description.server_type},
                   "newDescription":
                        {"address": "%s:%s" % actual.new_description.address,
                         "all_hosts": actual.new_description.all_hosts,
                         "server_type": actual.new_description.server_type}}
    import copy
    expected_dict = copy.deepcopy(expected)

    print("\tACTUAL")
    pretty_print(actual_dict)
    print("\tEXPECTED")
    pretty_print(expected_dict)


def compare_server_descriptions(self, expected, actual):
    self.assertEqual(expected['address'], "%s:%s" % actual.address)
    self.assertEqual(
        SERVER_TYPE.__getattribute__(expected['type']), actual.server_type)
    expected_hosts = set(
        expected['arbiters'] + expected['passives'] + expected['hosts'])
    self.assertEqual(expected_hosts,
                     set("%s:%s" % s for s in actual.all_hosts))


def compare_topology_descriptions(self, expected, actual):
    self.assertEqual(TOPOLOGY_TYPE.__getattribute__(expected['topologyType']),
                     actual.topology_type)
    expected = expected['servers']
    actual = actual.server_descriptions()
    self.assertEqual(len(expected), len(actual))
    for exp_server in expected:
        found = False
        for address, actual_server in actual.items():
            if "%s:%s" % address == exp_server['address']:
                found = True
                compare_server_descriptions(self, exp_server, actual_server)
        self.assertTrue(found,
                        "Error: expected server not found in actual results")


def compare_events(self, expected_dict, actual):
    if not expected_dict:
        self.assertTrue(False)
    if not actual:
        self.assertTrue(False, "Event published was None")

    expected_type, expected = list(expected_dict.items())[0]

    if expected_type == "server_opening_event":
        self.assertTrue(isinstance(actual, monitoring.ServerOpeningEvent))
        self.assertEqual(expected['address'], "%s:%s" % actual.server_address)

    elif expected_type == "server_description_changed_event":

        # print_server_desc(actual, expected)
        self.assertTrue(isinstance(actual,
                                   monitoring.ServerDescriptionChangedEvent))
        # self.assertEqual(expected['address'], "%s:%s" % actual.server_address)
        compare_server_descriptions(self, expected['newDescription'],
                                    actual.new_description)
        compare_server_descriptions(self, expected['previousDescription'],
                                    actual.previous_description)

    elif expected_type == "server_closed_event":
        self.assertTrue(isinstance(actual, monitoring.ServerClosedEvent))
        self.assertEqual(expected['address'], "%s:%s" % actual.server_address)

    elif expected_type == "topology_opening_event":
        self.assertTrue(isinstance(actual, monitoring.TopologyOpenedEvent))

    elif expected_type == "topology_description_changed_event":
        self.assertTrue(isinstance(actual,
                                   monitoring.TopologyDescriptionChangedEvent))
        compare_topology_descriptions(self, expected['newDescription'],
                                      actual.new_description)
        compare_topology_descriptions(self, expected['previousDescription'],
                                      actual.previous_description)

    elif expected_type == "topology_closed_event":
        self.assertTrue(isinstance(actual, monitoring.TopologyClosedEvent))

    else:
        self.assertTrue(
            False,
            "Incorrect event: expected %s, actual %s" % (expected_type, actual)
        )


def compare_multiple_events(self, i, expected_results):
    events_in_a_row = []
    j = i
    while(j < len(expected_results) and isinstance(
            self.all_listener.results[j],
            self.all_listener.results[i].__class__)):
        events_in_a_row.append(self.all_listener.results[j])
        j += 1
    for event in events_in_a_row:
        passed = False
        for k in range(i, j):
            try:
                compare_events(self, expected_results[k], event)
            except AssertionError:
                passed = False
            else:
                passed = True
                expected_results[k] = None
                break
        if not passed:
            self.assertTrue(False, "Unexpected event %s" % (event.__class__.__name__))
    return j


class TestAllScenarios(unittest.TestCase):

    @classmethod
    @client_context.require_connection
    def setUp(cls):
        cls.all_listener = AllEventListener()
        cls.saved_listeners = monitoring._LISTENERS
        monitoring._LISTENERS = monitoring._Listeners([], [], [], [])

    @classmethod
    def tearDown(cls):
        monitoring._LISTENERS = cls.saved_listeners


def create_test(scenario_def):
    def run_scenario(self):
        responses = (r for r in scenario_def['phases'][0]['responses'])

        class MockMonitor(Monitor):
            def __init__(self, server_description, topology, pool,
                         topology_settings):
                """Have to copy entire constructor from Monitor so that we can
                override _run."""

                self._server_description = server_description
                self._pool = pool
                self._settings = topology_settings
                self._avg_round_trip_time = MovingAverage()
                self._listeners = self._settings._pool_options.event_listeners
                self._publish = self._listeners is not None

                def target():
                    monitor = self_ref()
                    if monitor is None:
                        return False
                    MockMonitor._run(monitor)  # Only change.
                    return True

                # TODO: note this change too.
                executor = periodic_executor.PeriodicExecutor(
                    interval=0.1,
                    min_interval=0.1,
                    target=target,
                    name="pymongo_server_monitor_thread")
                self._executor = executor
                self_ref = weakref.ref(self, executor.close)
                self._topology = weakref.proxy(topology, executor.close)

            def _run(self):
                try:
                    # TODO: check if this is the correct server to be getting this response?
                    if self._server_description.address != ('a', 27017):
                        return
                    response = next(responses)[1]
                    isMaster = IsMaster(response)
                    self._server_description = ServerDescription(
                        address=self._server_description.address,
                        ismaster=isMaster)
                    self._topology.on_change(self._server_description)
                except (ReferenceError, StopIteration):
                    # Topology was garbage-collected.
                    self.close()

        m = single_client(h=scenario_def['uri'],
                          event_listeners=(self.all_listener,),
                          _monitor_class=MockMonitor)

        expected_results = scenario_def['phases'][0]['outcome']['events']

        # TODO: add this to "client_knobs"
        common.EVENTS_QUEUE_FREQUENCY = 0.1
        expected_len = len(expected_results)
        wait_until(lambda: len(self.all_listener.results) >= expected_len,
                   "publish all events", timeout=15)


        try:
            i = 0
            while i < expected_len:
                result = self.all_listener.results[i] if len(
                    self.all_listener.results) > i else None
                # print("\t%s" % dict((name, getattr(result, name)) for name in dir(
                #     result) if not name.startswith('_')))
                # Order of ServerOpening/ClosedEvents doesn't matter
                if (isinstance(result, monitoring.ServerOpeningEvent) or
                    isinstance(result, monitoring.ServerClosedEvent)):
                    i = compare_multiple_events(self, i, expected_results)
                else:
                    compare_events(self, expected_results[i], result)
                    i += 1

        finally:
            m.close()
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

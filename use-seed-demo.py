# This script is designed for support to understand what's going on :)

# Register all examples currently at:
# http://api.mongodb.com/python/current/api/pymongo/monitoring.html

import logging
import sys
import time
from pymongo import monitoring
from pymongo import MongoClient

# Change default logging to the console from WARNING to INFO
logging.getLogger().setLevel(logging.INFO)

try:
    from termcolor import colored
except ImportError:
    print('Could not import termcolor, text will not be colored')

    def colored(text, _):
        return text
else:
    _warning = logging.warning
    _info = logging.info

    def red_warnings(text):
        _warning(colored(text, 'red'))
    logging.warning = red_warnings

    def green_success(text):
        if 'changed type' in text:
            _info(colored(text, 'green'))
        else:
            _info(text)
    logging.info = green_success

if '-h' in sys.argv or '--help' in sys.argv:
    message = '\n'.join((
        'Demonstrates how to view output from useSeedList=true',
        'Usage: ',
        '\tpython use-seed-demo.py <connection-string>',
    ))
    print(message)
    exit(0)
if len(sys.argv) > 1:
    connection_string = sys.argv[1]
else:
    connection_string = 'mongodb://localhost:27017/?useSeedList=true'


class CommandLogger(monitoring.CommandListener):

    def started(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} started on server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds".format(event))

    def failed(self, event):
        logging.info("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds".format(event))


class ServerLogger(monitoring.ServerListener):
    def opened(self, event):
        logging.info("Server {0.server_address} added to topology "
                     "{0.topology_id}".format(event))

    def description_changed(self, event):
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            logging.info(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event):
        logging.warning("Server {0.server_address} removed from topology "
                        "{0.topology_id}".format(event))


class HeartbeatLogger(monitoring.ServerHeartbeatListener):

    def started(self, event):
        logging.info("Heartbeat sent to server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        # The reply.document attribute was added in PyMongo 3.4.
        logging.info("Heartbeat to server {0.connection_id} "
                     "succeeded with reply "
                     "{0.reply.document}".format(event))

    def failed(self, event):
        logging.warning("Heartbeat to server {0.connection_id} "
                        "failed with error {0.reply}".format(event))


class TopologyLogger(monitoring.TopologyListener):

    def opened(self, event):
        logging.info("Topology with id {0.topology_id} "
                     "opened".format(event))

    def description_changed(self, event):
        logging.info("Topology description updated for "
                     "topology id {0.topology_id}".format(event))
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            logging.info(
                "Topology {0.topology_id} changed type from "
                "{0.previous_description.topology_type_name} to "
                "{0.new_description.topology_type_name}".format(event))
        # The has_writable_server and has_readable_server methods
        # were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            logging.warning("No writable servers available.")
        if not event.new_description.has_readable_server():
            logging.warning("No readable servers available.")

    def closed(self, event):
        logging.info("Topology with id {0.topology_id} "
                     "closed".format(event))

monitoring.register(CommandLogger())
monitoring.register(ServerLogger())
# monitoring.register(HeartbeatLogger())
monitoring.register(TopologyLogger())


# Then added MongoClient and our new useSeedList option, based on:
# http://api.mongodb.com/python/current/tutorial.html
client = MongoClient(connection_string)


# Run for at least this long, in seconds
run_for = 10
message = colored('This useSeedList=true demo will run for {} seconds'
                  .format(run_for), 'cyan')
print(message)
count = 0
while count < run_for:
    count += 1
    time.sleep(1)

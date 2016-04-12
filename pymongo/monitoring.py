# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Tools to monitor driver events.

Use :func:`register` to register global listeners for specific events.
Listeners must inherit from a subclass of :class:`EventListener` and implement
the correct functions for that class.

For example, a simple command logger might be implemented like this::

    import logging

    from pymongo import monitoring

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
                         "failed in {s".format(event))

    monitoring.register(CommandLogger())

Command listeners can also be registered per instance of
:class:`~pymongo.mongo_client.MongoClient`::

    client = MongoClient(command_listeners=[CommandLogger()],
                         server_listeners=[ServerLogger()])

Note that previously registered global listeners are automatically included when
configuring per client event listeners. Registering a new global listener will
not add that listener to existing client instances.

.. note:: Events are delivered **synchronously**. Application threads block
  waiting for event handlers (e.g. :meth:`~CommandListener.started`) to
  return. Care must be taken to ensure that your event handlers are efficient
  enough to not adversely affect overall application performance.

.. warning:: The command documents published through this API are *not* copies.
  If you intend to modify them in any way you must copy them in your event
  handler first.
"""

import sys
import traceback

from collections import namedtuple, Sequence

_Listeners = namedtuple('Listeners',
                        ('command_listeners', 'server_listeners',
                         'server_heartbeat_listeners', 'topology_listeners'))

_LISTENERS = _Listeners([], [], [], [])


#TODO: java doesn't share inheritance between command_listener classes. Also renamed Topology to Cluster.
class EventListener(object):
    """Abstract base class for all event listeners. """


class BasicEventListener(EventListener):
    """Abstract base class for listeners that listen for started, succeeded,
    and failed events."""

    def started(self, event):
        """Abstract method to handle a "started" event, such as
        `CommandStartedEvent` or `ServerHeartbeatStartedEvent`.

        :Parameters:
          - `event`: An instance of a "started" event.
        """
        raise NotImplementedError

    def succeeded(self, event):
        """Abstract method to handle a "succeeded" event, such as
        `CommandSucceededEvent` or `ServerHeartbeatSuccessfulEvent`.

        :Parameters:
          - `event`: An instance of a "succeeded" event.
        """
        raise NotImplementedError

    def failed(self, event):
        """Abstract method to handle a "failed" event, such as
        `CommandFailedEvent` or `ServerHeartbeatFailedEvent`.

        :Parameters:
          - `event`: An instance of a "failed" event.
        """
        raise NotImplementedError


class ClusterListener(EventListener):
    """Abstract base class for listeners that listen for "opened", "changed",
    and "closed" events."""

    def opened(self, event):
        """Abstract method to handle a "opened" event, such as
        `ServerOpenedEvent` or `TopologyOpenedEvent`.

        :Parameters:
          - `event`: An instance of a "opened" event.
        """
        raise NotImplementedError

    def description_changed(self, event):
        """Abstract method to handle a "description changed" event, such as
        `ServerDescriptionChangedEvent` or `TopologyDescriptionChangedEvent`.

        :Parameters:
          - `event`: An instance of a "description changed" event.
        """
        raise NotImplementedError

    def closed(self, event):
        """Abstract method to handle a "closed" event, such as
        `ServerClosedEvent` or `TopologyClosedEvent`.

        :Parameters:
          - `event`: An instance of a "closed" event.
        """
        raise NotImplementedError


class CommandListener(BasicEventListener):
    """Abstract base class for command listeners.
    Expects `CommandStartedEvent`, `CommandSucceededEvent`,
    and `CommandFailedEvent`"""


class ServerHeartbeatListener(BasicEventListener):
    """Abstract base class for server heartbeat listeners.
    Expects `ServerHeartbeatStartedEvent`, `ServerHeartbeatSucceededEvent`,
    and `ServerHeartbeatFailedEvent`."""


class TopologyListener(ClusterListener):
    """Abstract base class for topology monitoring listeners.
    Expects `TopologyOpenedEvent`, `TopologyDescriptionChangedEvent`, and
    `TopologyClosedEvent`."""


class ServerListener(ClusterListener):
    """Abstract base class for server listeners.
    Expects `ServerOpenedEvent`, `ServerDescriptionChangedEvent`, and
    `ServerClosedEvent`."""


def _to_micros(dur):
    """Convert duration 'dur' to microseconds."""
    if hasattr(dur, 'total_seconds'):
        return int(dur.total_seconds() * 10e5)
    # Python 2.6
    return dur.microseconds + (dur.seconds + dur.days * 24 * 3600) * 1000000


def _validate_command_listeners(option, listeners):
    """Validate command listeners"""
    if not isinstance(listeners, Sequence):
        raise TypeError("%s must be a list or tuple" % (option,))
    for listener in listeners:
        if not isinstance(listener, CommandListener):
            raise TypeError("Command listeners for %s must be a subclass of "
                            "CommandListener" % option)
    return listeners


def _validate_server_listeners(option, listeners):
    """Validate server listeners"""
    if not isinstance(listeners, Sequence):
        raise TypeError("%s must be a list or tuple" % (option,))
    for listener in listeners:
        if not isinstance(listener, ServerListener):
            raise TypeError("Server listeners for %s must be a subclass of "
                            "ServerListener" % option)
    return listeners


def _validate_server_heartbeat_listeners(option, listeners):
    """Validate server heartbeat listeners"""
    if not isinstance(listeners, Sequence):
        raise TypeError("%s must be a list or tuple" % (option,))
    for listener in listeners:
        if not isinstance(listener, ServerHeartbeatListener):
            raise TypeError("Server heartbeat listeners for %s must be a "
                            "subclass of ServerHeartbeatListener" % option)
    return listeners


def _validate_topology_listeners(option, listeners):
    """Validate topology listeners"""
    if not isinstance(listeners, Sequence):
        raise TypeError("%s must be a list or tuple" % (option,))
    for listener in listeners:
        if not isinstance(listener, TopologyListener):
            raise TypeError("Topology listeners for %s must be a subclass of "
                            "TopologyListener" % option)
    return listeners


def register(command_listener=None, server_listener=None,
             server_heartbeat_listener=None, topology_listener=None):
    """Register a global event listener.

    :Parameters:
      - `command_listener`: A subclass of :class:`CommandListener`.
      - `server_listener`: A subclass of :class:`ServerListener`.
      - `server_heartbeat_listener`: A subclass of
         :class:`ServerHeartbeatListener`.
      - `topology_listener`: A subclass of :class:`TopologyListener`.
    """
    if command_listener is not None:
        _validate_command_listeners('command_listener', [command_listener])
        _LISTENERS.command_listeners.append(command_listener)

    if server_listener is not None:
        _validate_server_listeners('server_listener', [server_listener])
        _LISTENERS.server_listeners.append(server_listener)

    if server_heartbeat_listener is not None:
        _validate_server_heartbeat_listeners(
            'server_heartbeat_listener', [server_heartbeat_listener])
        _LISTENERS.server_heartbeat_listeners.append(server_heartbeat_listener)

    if topology_listener is not None:
        _validate_topology_listeners('topology_listener', [topology_listener])
        _LISTENERS.topology_listeners.append(topology_listener)


def _handle_exception():
    """Print exceptions raised by subscribers to stderr."""
    # Heavily influenced by logging.Handler.handleError.

    # See note here:
    # https://docs.python.org/3.4/library/sys.html#sys.__stderr__
    if sys.stderr:
        einfo = sys.exc_info()
        try:
            traceback.print_exception(einfo[0], einfo[1], einfo[2],
                                      None, sys.stderr)
        except IOError:
            pass
        finally:
            del einfo

# Note - to avoid bugs from forgetting which if these is all lowercase and
# which are camelCase, and at the same time avoid having to add a test for
# every command, use all lowercase here and test against command_name.lower().
_SENSITIVE_COMMANDS = set(
    ["authenticate", "saslstart", "saslcontinue", "getnonce", "createuser",
     "updateuser", "copydbgetnonce", "copydbsaslstart", "copydb"])


class _CommandEvent(object):
    """Base class for command events."""

    __slots__ = ("__cmd_name", "__rqst_id", "__conn_id", "__op_id")

    def __init__(self, command_name, request_id, connection_id, operation_id):
        self.__cmd_name = command_name
        self.__rqst_id = request_id
        self.__conn_id = connection_id
        self.__op_id = operation_id

    @property
    def command_name(self):
        """The command name."""
        return self.__cmd_name

    @property
    def request_id(self):
        """The request id for this operation."""
        return self.__rqst_id

    @property
    def connection_id(self):
        """The address (host, port) of the server this command was sent to."""
        return self.__conn_id

    @property
    def operation_id(self):
        """An id for this series of events or None."""
        return self.__op_id


class CommandStartedEvent(_CommandEvent):
    """Event published when a command starts.

    :Parameters:
      - `command`: The command document.
      - `database_name`: The name of the database this command was run against.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__cmd", "__db")

    def __init__(self, command, database_name, *args):
        if not command:
            raise ValueError("%r is not a valid command" % (command,))
        # Command name must be first key.
        command_name = next(iter(command))
        super(CommandStartedEvent, self).__init__(command_name, *args)
        if command_name.lower() in _SENSITIVE_COMMANDS:
            self.__cmd = {}
        else:
            self.__cmd = command
        self.__db = database_name

    @property
    def command(self):
        """The command document."""
        return self.__cmd

    @property
    def database_name(self):
        """The name of the database this command was run against."""
        return self.__db


class CommandSucceededEvent(_CommandEvent):
    """Event published when a command succeeds.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `reply`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__duration_micros", "__reply")

    def __init__(self, duration, reply, command_name,
                 request_id, connection_id, operation_id):
        super(CommandSucceededEvent, self).__init__(
            command_name, request_id, connection_id, operation_id)
        self.__duration_micros = _to_micros(duration)
        if command_name.lower() in _SENSITIVE_COMMANDS:
            self.__reply = {}
        else:
            self.__reply = reply

    @property
    def duration_micros(self):
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def reply(self):
        """The server failure document for this operation."""
        return self.__reply


class CommandFailedEvent(_CommandEvent):
    """Event published when a command fails.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `failure`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__duration_micros", "__failure")

    def __init__(self, duration, failure, *args):
        super(CommandFailedEvent, self).__init__(*args)
        self.__duration_micros = _to_micros(duration)
        self.__failure = failure

    @property
    def duration_micros(self):
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def failure(self):
        """The server failure document for this operation."""
        return self.__failure


class _ServerDescriptionEvent(object):
    """Base class for server description events."""

    __slots__ = ("__server_address", "__topology_id")

    def __init__(self, server_address, topology_id):
        self.__server_address = server_address
        self.__topology_id = topology_id

    @property
    def server_address(self):
        """The address (host/port pair) of the server"""
        return self.__server_address

    @property
    def topology_id(self):
        """A unique identifier for the topology."""
        return self.__topology_id


class ServerDescriptionChangedEvent(_ServerDescriptionEvent):
    """Published when server description changes, but does NOT include
    changes to the RTT."""

    __slots__ = ('__previous_description', '__new_description')

    def __init__(self, previous_description, new_description, *args):
        super(ServerDescriptionChangedEvent, self).__init__(*args)
        self.__previous_description = previous_description
        self.__new_description = new_description

    @property
    def previous_description(self):
        """The previous server description."""
        return self.__previous_description

    @property
    def new_description(self):
        """The new server description."""
        return self.__new_description


class ServerOpenedEvent(_ServerDescriptionEvent):
    """Published when server is initialized."""


class ServerClosedEvent(_ServerDescriptionEvent):
    """Published when server is closed."""


class _TopologyDescriptionEvent(object):
    """Base class for topology description events"""

    __slots__ = ('__topology_id')

    def __init__(self, topology_id):
        self.__topology_id = topology_id

    @property
    def topology_id(self):
        """A unique identifier for the topology."""
        return self.__topology_id


class TopologyDescriptionChangedEvent(_TopologyDescriptionEvent):
    """Published when topology description changes."""

    __slots__ = ('__previous_description', '__new_description')

    def __init__(self, previous_description,  new_description, *args):
        super(TopologyDescriptionChangedEvent, self).__init__(*args)
        self.__previous_description = previous_description
        self.__new_description = new_description

    @property
    def previous_description(self):
        """The old topology description."""
        return self.__previous_description

    @property
    def new_description(self):
        """The new topology description."""
        return self.__new_description


class TopologyOpenedEvent(_TopologyDescriptionEvent):
    """Published when topology is initialized."""


class TopologyClosedEvent(_TopologyDescriptionEvent):
    """Published when topology is closed."""


class _ServerHeartbeatEvent(object):
    """Base class for server heartbeat events"""

    __slots__ = ('__connection_id')

    def __init__(self, connection_id):
        self.__connection_id = connection_id

    @property
    def connection_id(self):
        """Returns the connection id for the command. The connection id is the
        unique identifier of the driver's Connection object that wraps the
        socket."""
        return self.__connection_id


class ServerHeartbeatStartedEvent(_ServerHeartbeatEvent):
    """Fired when the server monitor's ismaster command is started -
    immediately before the ismaster command is serialized into raw BSON
    and written to the socket."""


class ServerHeartbeatSucceededEvent(_ServerHeartbeatEvent):
    """Fired when the server monitor's ismaster succeeds."""

    __slots__ = ('__duration', '__reply')

    def __init__(self, duration, reply, *args):
        super(ServerHeartbeatSucceededEvent, self).__init__(*args)
        self.__duration = duration
        self.__reply = reply

    @property
    def duration(self):
        """Returns the execution time of the event in the highest possible
        resolution for the platform. The calculated value MUST be the time to
        send the message and receive the reply from the server, including BSON
        serialization and deserialization. The name can imply the units in
        which the value is returned, i.e. durationMS, durationNanos. The time
        measurement used MUST be the same measurement used for the RTT
        calculation."""
        return self.__duration

    @property
    def reply(self):
        """The command reply."""
        return self.__reply


class ServerHeartbeatFailedEvent(_ServerHeartbeatEvent):
    """Fired when the server monitor's ismaster fails, either with an "ok: 0"
    or a socket exception."""

    __slots__ = ('__duration', '__reply')

    def __init__(self, duration, reply, *args):
        super(ServerHeartbeatFailedEvent, self).__init__(*args)
        self.__duration = duration
        self.__reply = reply

    @property
    def duration(self):
        """Returns the execution time of the event in the highest possible
        resolution for the platform. The calculated value MUST be the time to
        send the message and receive the reply from the server, including BSON
        serialization and deserialization. The name can imply the units in
        which the value is returned, i.e. durationMS, durationNanos. The time
        measurement used MUST be the same measurement used for the RTT
        calculation."""
        return self.__duration

    @property
    def reply(self):
        """The command reply."""
        return self.__reply


class _CommandListeners(object):
    """Configure command listeners for a client instance.

    Any command listeners registered globally are included by default.

    :Parameters:
      - `listeners`: A list of command listeners.
    """
    def __init__(self, listeners):
        self.__command_listeners = _LISTENERS.command_listeners[:]
        if listeners is not None:
            self.__command_listeners.extend(listeners)
        self.__enabled = bool(self.__command_listeners)

    @property
    def enabled(self):
        """Are any CommandListener instances registered?"""
        return self.__enabled

    @property
    def command_listeners(self):
        """List of registered command listeners."""
        return self.__command_listeners[:]

    def publish_command_start(self, command, database_name,
                              request_id, connection_id, op_id=None):
        """Publish a CommandStartedEvent to all command listeners.

        :Parameters:
          - `command`: The command document.
          - `database_name`: The name of the database this command was run
            against.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandStartedEvent(
            command, database_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.started(event)
            except Exception:
                _handle_exception()


    def publish_command_success(self, duration, reply, command_name,
                                request_id, connection_id, op_id=None):
        """Publish a CommandSucceededEvent to all command listeners.

        :Parameters:
          - `duration`: The command duration as a datetime.timedelta.
          - `reply`: The server reply document.
          - `command_name`: The command name.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandSucceededEvent(
            duration, reply, command_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.succeeded(event)
            except Exception:
                _handle_exception()


    def publish_command_failure(self, duration, failure, command_name,
                                request_id, connection_id, op_id=None):
        """Publish a CommandFailedEvent to all command listeners.

        :Parameters:
          - `duration`: The command duration as a datetime.timedelta.
          - `failure`: The server reply document or failure description
            document.
          - `command_name`: The command name.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandFailedEvent(
            duration, failure, command_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.failed(event)
            except Exception:
                _handle_exception()


class _ServerHeartbeatListeners(object):
    """Configure server heartbeat listeners for a client instance.

    Any server heartbeat listeners registered globally are included by default.

    :Parameters:
      - `listeners`: A list of server heartbeat listeners.
    """
    def __init__(self, listeners):
        self.__server_heartbeat_listeners = _LISTENERS.server_heartbeat_listeners[:]
        if listeners is not None:
            self.__server_heartbeat_listeners.extend(listeners)
        self.__enabled = bool(self.__server_heartbeat_listeners)

    @property
    def enabled(self):
        """Are any ServerHeartbeatListener instances registered?"""
        return self.__enabled

    @property
    def server_heartbeat_listeners(self):
        """List of registered server heartbeat listeners."""
        return self.__server_heartbeat_listeners[:]

    def publish_server_heartbeat_started(self, connection_id):
        """Publish a ServerHeartbeatStartedEvent to all server heartbeat
        listeners.

        :Parameters:
         - `connection_id`: The unique identifier of the driver's Connection
           object that wraps the socket.
        """
        event = ServerHeartbeatStartedEvent(connection_id)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.started(event)
            except Exception:
                _handle_exception()

    def publish_server_heartbeat_succeeded(self, connection_id, duration,
                                           reply):
        """Publish a ServerHeartbeatSucceededEvent to all server heartbeat
        listeners.

        :Parameters:
         - `connection_id`: The unique identifier of the driver's Connection
            object that wraps the socket.
         - `duration`: The execution time of the event in the highest possible
            resolution for the platform.
         - `reply`: The command reply.
         """
        event = ServerHeartbeatSucceededEvent(duration, reply, connection_id)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.succeeded(event)
            except Exception:
                _handle_exception()

    def publish_server_heartbeat_failed(self, connection_id, duration,
                                           reply):
        """Publish a ServerHeartbeatFailedEvent to all server heartbeat
        listeners.

        :Parameters:
         - `connection_id`: The unique identifier of the driver's Connection
            object that wraps the socket.
         - `duration`: The execution time of the event in the highest possible
            resolution for the platform.
         - `reply`: The command reply.
         """
        event = ServerHeartbeatSucceededEvent(duration, reply, connection_id)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.failed(event)
            except Exception:
                _handle_exception()


class _ServerListeners(object):
    """Configure server listeners for a client instance.

    Any server listeners registered globally are included by default.

    :Parameters:
      - `listeners`: A list of server listeners.
    """
    def __init__(self, listeners):
        self.__server_listeners = _LISTENERS.server_listeners[:]
        if listeners is not None:
            self.__server_listeners.extend(listeners)
        self.__enabled = bool(self.__server_listeners)

    @property
    def enabled(self):
        """Are any ServerListener instances registered?"""
        return self.__enabled

    @property
    def server_listeners(self):
        """List of registered server listeners."""
        return self.__server_listeners[:]

    def publish_server_opened(self, server_address, topology_id):
        """Publish a ServerOpenedEvent to all server listeners.

        :Parameters:
         - `server_address`: The address (host/port pair) of the server.
         - `topology_id`: A unique identifier for the topology.
        """
        event = ServerOpenedEvent(server_address, topology_id)
        for subscriber in self.__server_listeners:
            try:
                subscriber.opened(event)
            except Exception:
                _handle_exception()

    def publish_server_closed(self, server_address, topology_id):
        """Publish a ServerClosedEvent to all server listeners.

        :Parameters:
         - `server_address`: The address (host/port pair) of the server.
         - `topology_id`: A unique identifier for the topology.
        """
        event = ServerClosedEvent(server_address, topology_id)
        for subscriber in self.__server_listeners:
            try:
                subscriber.closed(event)
            except Exception:
                _handle_exception()

    def publish_server_description_changed(self, previous_description,
                                           new_description):
        """Publish a ServerDescriptionChangedEvent to all server listeners.

        :Parameters:
         - `previous_description`: The previous server description.
         - `new_description`: The new server description.
        """
        event = ServerDescriptionChangedEvent(previous_description,
                                              new_description)
        for subscriber in self.__server_listeners:
            try:
                subscriber.description_changed(event)
            except Exception:
                _handle_exception()


class _TopologyListeners(object):
    """Configure topology listeners for a client instance.

    Any topology listeners registered globally are included by default.

    :Parameters:
      - `listeners`: A list of topology listeners.
    """
    def __init__(self, listeners):
        self.__topology_listeners = _LISTENERS.topology_listeners[:]
        if listeners is not None:
            self.__topology_listeners.extend(listeners)
        self.__enabled = bool(self.__topology_listeners)

    @property
    def enabled(self):
        """Are any TopologyListener instances registered?"""
        return self.__enabled

    @property
    def topology_listeners(self):
        """List of registered topology listeners."""
        return self.__topology_listeners[:]

    def publish_topology_opened(self, topology_id):
        """Publish a TopologyOpenedEvent to all topology listeners.

        :Parameters:
         - `topology_id`: A unique identifier for the topology.
        """
        event = TopologyOpenedEvent(topology_id)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.opened(event)
            except Exception:
                _handle_exception()

    def publish_topology_closed(self, topology_id):
        """Publish a TopologyClosedEvent to all topology listeners.

        :Parameters:
         - `topology_id`: A unique identifier for the topology.
        """
        event = TopologyClosedEvent(topology_id)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.closed(event)
            except Exception:
                _handle_exception()

    def publish_topology_description_changed(self, previous_description,
                                             new_description):
        """Publish a TopologyDescriptionChangedEvent to all topology listeners.

        :Parameters:
         - `previous_description`: The previous topology description.
         - `new_description`: The new topology description.
        """
        event = TopologyDescriptionChangedEvent(previous_description,
                                                new_description)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.description_changed(event)
            except Exception:
                _handle_exception()

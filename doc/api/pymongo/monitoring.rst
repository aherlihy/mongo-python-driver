:mod:`monitoring` -- Tools for monitoring driver events.
========================================================

.. automodule:: pymongo.monitoring
   :synopsis: Tools for monitoring driver events.

   .. autofunction:: register(listener)
   .. autoclass:: CommandListener
      :members:
   .. autoclass:: ServerListener
      :members:
   .. autoclass:: ServerHeartbeatListener
      :members:
   .. autoclass:: TopologyListener
      :members:
   .. autoclass:: CommandStartedEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandSucceededEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandFailedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerDescriptionChangedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerOpenedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerClosedEvent
      :members:
   .. autoclass:: TopologyDescriptionChangedEvent
      :members:
      :inherited-members: TopologyOpenedEvent
   .. autoclass::
      :members:
      :inherited-members: TopologyClosedEvent
   .. autoclass::
      :members:
      :inherited-members: ServerHeartbeatStartedEvent
   .. autoclass::
      :members:
      :inherited-members: ServerHeartbeatSucceededEvent
   .. autoclass::
      :members:
      :inherited-members: ServerHeartbeatFailedEvent

Architecture Guide
==================

Each agent consists of three major components:

-   Sources are active components that receive data for some other application producing that data.

-   Channels are passive components that buffer data received by the agent, behaving like queues.

-   Sinks poll their respective channels continuously to read and remove events.

 .. image:: images/ingestion_architecture.jpg
    :width: 70%
    :align: center


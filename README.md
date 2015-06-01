KafkaBase
===========

Defines common functionality to Kafka blocks.

Kafka blocks require running Kafka and Zookeeper servers
see: http://kafka.apache.org/documentation.html#quickstart

**Important** In order for topics to be created, Kafka server needs to be
configured with setting 'auto.create.topics.enable' set to true.
see: https://kafka.apache.org/08/configuration.html

Properties
--------------

-   **host**: Kafka server host.
-   **port**: Kafka server port.
-   **topic**: Topic to use.

Dependencies
----------------

-   [kafka-python](https://github.com/mumrah/kafka-python)

Commands
----------------
None

Input
-------
None

Output
---------
None

----------------

KafkaProducer
===========

A block that receives signals and sends them to Kafka server.

Properties
--------------

-   None

Dependencies
----------------

-   [kafka-python](https://github.com/mumrah/kafka-python)

Commands
----------------
None

Input
-------
NIO signals.


----------------

KafkaConsumer
===========

Consumes Kafka messages.

Properties
--------------

-   **group**: Group to consume Kafka messages from

Dependencies
----------------

-   [kafka-python](https://github.com/mumrah/kafka-python)

Commands
----------------
None

Input
-------
Kafka messages.

Output
---------
Kafka message as a NIO signal.

----------------

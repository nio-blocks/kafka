KafkaBase
===========

Defines common functionality to Kafka blocks.

Kafka blocks require running Kafka and Zookeeper servers
see: http://kafka.apache.org/documentation.html#quickstart

Starting servers:
--------------

bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties


**Important** In order for topics to be created automatically, Kafka server needs to be
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

KafkaBatchProducer
===========

A block that receives signals and sends them to Kafka server. This blocks
uses Kafka infrastructure to send signals, in order to receive signals
successfully receiving block must be of type KafkaBatchConsumer

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

KafkaBatchConsumer
===========

Consumes Kafka messages sent from a KafkaBatchProducer block

Properties
--------------

-   **group**: Group to consume Kafka messages from
-   **msg_buffer_size**: maximum length of buffer used to capture signals, this value cannot be smaller than size of all signals sent from KafkaBatchProducer

Dependencies
----------------

-   [kafka-python](https://github.com/mumrah/kafka-python)

Commands
----------------
None

Input
-------
Kafka message sent from a KafkaBatchProducer.

Output
---------
Kafka message as a NIO signal.

----------------

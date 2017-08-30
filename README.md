KafkaBatchConsumer
==================
Consumes Kafka messages sent from a KafkaBatchProducer block.

Properties
----------
- **group**: Group to consume Kafka messages from.
- **host**: Kafka server host.
- **max_msg_count**: Max message count.
- **msg_buffer_size**: maximum length of buffer used to capture signals, this value cannot be smaller than size of all signals sent from KafkaBatchProducer
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
- **default**: Kafka message sent from a KafkaBatchProducer.

Outputs
-------
- **default**: Kafka message as a NIO signal.

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

KafkaBatchProducer
==================
A block that receives signals and sends them to Kafka server. This blocks uses Kafka infrastructure to send signals, in order to receive signals successfully receiving block must be of type KafkaBatchConsumer

Properties
----------
- **host**: Kafka server host.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: 

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

KafkaConsumer
=============
Consumes Kafka messages.

Properties
----------
- **group**: Group to consume Kafka messages from.
- **host**: Kafka server host.
- **max_msg_count**: Max message count.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
- **default**: Kafka messages.

Outputs
-------
- **default**: Kafka message as a NIO signal.

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

KafkaProducer
=============
A block that receives signals and sends them to Kafka server.

Properties
----------
- **host**: Kafka server host.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: 

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

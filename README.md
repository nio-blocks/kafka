KafkaBatchConsumer
==================
Consumes Kafka messages sent from a KafkaBatchProducer block.

Properties
----------
- **group**: Group to consume Kafka messages from.
- **host**: Kafka server host.
- **max_msg_count**: Maximum message count.
- **msg_buffer_size**: Maximum length of buffer used to capture signals. This value cannot be smaller than the size of all signals sent from KafkaBatchProducer.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
None

Outputs
-------
- **default**: Kafka message as a nio signal.

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

***

KafkaBatchProducer
==================
A block that receives signals and sends them to the Kafka server. This blocks uses Kafka infrastructure to send signals. In order to receive signals successfully, there must be a receiving block of type KafkaBatchConsumer.

Properties
----------
- **host**: Kafka server host.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
- **default**: A list of signals coming from a KafkaBatchConsumer block.

Outputs
-------
None

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

***

KafkaConsumer
=============
Consumes Kafka messages.

Properties
----------
- **group**: Group to consume Kafka messages from.
- **host**: Kafka server host.
- **max_msg_count**: Maximum message count.
- **port**: Kafka server port.
- **topic**: Topic to use.

Inputs
------
None

Outputs
-------
- **default**: Kafka message as a nio signal.

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)

***

KafkaProducer
=============
A block that receives signals and sends them to the Kafka server.

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
None

Commands
--------
None

Dependencies
------------
-   [kafka-python](https://github.com/mumrah/kafka-python)


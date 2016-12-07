import pickle

from kafka.consumer import SimpleConsumer

from nio.properties import IntProperty, VersionProperty
from nio.util.discovery import discoverable

from .kafka_consumer_block import KafkaConsumer


@discoverable
class KafkaBatchConsumer(KafkaConsumer):

    """ A block for consuming Kafka Batch messages sent from a
    KafkaBatchProducer
    """
    version = VersionProperty(version='0.1.0')
    msg_buffer_size = IntProperty(title='Message Buffer Size',
                                  default=1000000,
                                  allow_none=False)

    def _receive_messages(self):
        while not self._stop_message_loop_event.is_set():
            # get kafka messages
            messages = self._consumer.get_messages(count=self.max_msg_count,
                                                   block=False)
            # if no timeout occurred, gather NIO signals
            if messages:
                signals = []
                for message in messages:
                    # grab directly NIO signals, this block must be in sync
                    # with KafkaNIOProducer
                    signals.extend(pickle.loads(message.message.value))

                self.notify_signals(signals)

        self.logger.debug("Exiting message loop")

    def _connect(self):
        super()._connect()
        self._consumer = SimpleConsumer(self._kafka,
                                        self._encoded_group,
                                        self._encoded_topic,
                                        fetch_size_bytes=self.msg_buffer_size,
                                        max_buffer_size=self.msg_buffer_size)

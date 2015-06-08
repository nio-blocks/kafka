import pickle
from kafka.consumer import SimpleConsumer

from .kafka_consumer_block import KafkaConsumer

from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import IntProperty


@Discoverable(DiscoverableType.block)
class KafkaBatchConsumer(KafkaConsumer):

    """ A block for consuming Kafka Batch messages sent from a
    KafkaBatchProducer
    """
    msg_buffer_size = IntProperty(title='Message Buffer Size',
                                  default=1000000,
                                  allow_none=False)

    def _receive_messages(self):
        signals = []
        while not self._stop_message_loop_event.is_set():
            # get kafka messages
            messages = self._consumer.get_messages(count=self.max_msg_count,
                                                   block=False)
            # if no timeout occurred, gather NIO signals
            if messages:
                for message in messages:
                    # grab directly NIO signals, this block must be in sync
                    # with KafkaNIOProducer
                    signals.extend(pickle.loads(message.message.value))

                # send signals in one shot
                self._send_signals(signals)

        self._logger.debug("Exiting message loop")

    def _connect(self):
        super()._connect()
        self._consumer = SimpleConsumer(self._kafka,
                                        self._encoded_group,
                                        self._encoded_topic,
                                        fetch_size_bytes=self.msg_buffer_size,
                                        max_buffer_size=self.msg_buffer_size)

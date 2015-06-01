from kafka.consumer import SimpleConsumer

from .kafka_base_block import KafkaBase

from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.signal.base import Signal
from nio.metadata.properties import StringProperty
from nio.modules.threading import spawn


@Discoverable(DiscoverableType.block)
class KafkaConsumer(KafkaBase):

    """ A block for consuming Kafka messages
    """
    group = StringProperty(title='Group', default="", allow_none=False)

    def __init__(self):
        super().__init__()
        self._consumer = None
        self._encoded_group = None

    def configure(self, context):
        super().configure(context)

        if not len(self.group):
            raise ValueError("Group cannot be empty")

        self._encoded_group = self.group.encode()
        self._connect()

    def start(self):
        super().start()

        spawn(self._receive_messages)

    def stop(self):
        self._disconnect()
        super().stop()

    def _parse_and_notify_message(self, message):
            attrs = dict()
            attrs["magic"] = message.message.magic
            attrs["attributes"] = message.message.attributes
            attrs["key"] = message.message.key
            attrs["value"] = message.message.value

            signal = Signal(attrs)
            self.notify_signals([signal])

    def _receive_messages(self):
        for message in self._consumer:
            self._parse_and_notify_message(message)

    def _connect(self):
        super()._connect()
        self._consumer = SimpleConsumer(self._kafka,
                                        self._encoded_group,
                                        self._encoded_topic)

    def _disconnect(self):
        if self._consumer:
            self._consumer.stop()
            self._consumer = None
        super()._disconnect()

    @property
    def connected(self):
        return super().connected and self._consumer

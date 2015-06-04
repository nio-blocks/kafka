import copy
from kafka.consumer import SimpleConsumer
from kafka.consumer.base import AUTO_COMMIT_MSG_COUNT

from .kafka_base_block import KafkaBase

from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.signal.base import Signal
from nio.metadata.properties import StringProperty, IntProperty
from nio.modules.threading import spawn, Event


@Discoverable(DiscoverableType.block)
class KafkaConsumer(KafkaBase):

    """ A block for consuming Kafka messages
    """
    group = StringProperty(title='Group', default="", allow_none=False)
    # use Kafka 'reasonable' value for our own message gathering and
    # signal delivery
    max_msg_count = IntProperty(title='Max message count',
                                default=AUTO_COMMIT_MSG_COUNT,
                                allow_none=False)

    def __init__(self):
        super().__init__()
        self._consumer = None
        self._encoded_group = None
        # message loop maintenance
        self._stop_message_loop_event = None
        self._message_loop_thread = None

    def configure(self, context):
        super().configure(context)

        if not len(self.group):
            raise ValueError("Group cannot be empty")

        self._encoded_group = self.group.encode()
        self._connect()

    def start(self):
        super().start()
        # start gathering messages
        self._stop_message_loop_event = Event()
        self._message_loop_thread = spawn(self._receive_messages)

    def stop(self):
        # stop gathering messages
        self._stop_message_loop_event.set()
        self._message_loop_thread.join()
        self._message_loop_thread = None

        # disconnect
        self._disconnect()
        super().stop()

    def _parse_message(self, message):
            attrs = dict()
            attrs["magic"] = message.message.magic
            attrs["attributes"] = message.message.attributes
            attrs["key"] = message.message.key
            attrs["value"] = message.message.value

            return Signal(attrs)

    def _receive_messages(self):
        signals = []
        while not self._stop_message_loop_event.is_set():
            # get kafka messages
            messages = self._consumer.get_messages(count=self.max_msg_count,
                                                   block=False)
            # if no timeout occurred, parse messages and convert to signals
            if messages:
                for message in messages:
                    # parse and save every signal
                    try:
                        signal = self._parse_message(message)
                    except Exception:
                        self._logger.exception("Failed to parse kafka message:"
                                               " '{0}'".format(message))
                        continue
                    signals.append(signal)

                # send signals in one shot
                self._send_signals(signals)

        self._logger.debug("Exiting message loop")

    def _send_signals(self, signals):
        if signals:
            self._logger.debug("Notifying: '{0}' signals".format(len(signals)))

            self.notify_signals(copy.deepcopy(signals))
            signals.clear()

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

from threading import Event
from kafka.consumer import SimpleConsumer
from kafka.consumer.base import AUTO_COMMIT_MSG_COUNT, FETCH_MAX_WAIT_TIME, \
    FETCH_MIN_BYTES

from nio import GeneratorBlock
from nio.properties import StringProperty, IntProperty, FloatProperty, \
    VersionProperty
from nio.signal.base import Signal
from nio.util.threading.spawn import spawn

from .kafka_base_block import KafkaBase


class KafkaConsumer(KafkaBase, GeneratorBlock):

    """ A block for consuming Kafka messages
    """
    version = VersionProperty("2.0.0")
    group = StringProperty(title="Group", default="", allow_none=False)
    # use Kafka 'reasonable' value for our own message gathering and
    # signal delivery
    max_msg_count = IntProperty(title="Max message count",
                                default=AUTO_COMMIT_MSG_COUNT,
                                advanced=True)
    fetch_size = IntProperty(title="Fetch Size (bytes)",
                             default=FETCH_MIN_BYTES,
                             advanced=True)
    timeout = FloatProperty(title="Timeout (seconds)",
                            default=FETCH_MAX_WAIT_TIME / 1000,
                            advanced=True)

    def __init__(self):
        super().__init__()
        self._consumer = None
        self._encoded_group = None
        # message loop maintenance
        self._stop_message_loop_event = None
        self._message_loop_thread = None

    def configure(self, context):
        super().configure(context)

        if not len(self.group()):
            raise ValueError("Group cannot be empty")

        self._encoded_group = self.group().encode()
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
        while not self._stop_message_loop_event.is_set():
            try:
                # get kafka messages
                messages = self._consumer.get_messages(
                    count=self.max_msg_count(),
                    block=False,
                    timeout=self.timeout())
            except Exception:
                self.logger.exception("Failure getting kafka messages")
                continue

            # if no timeout occurred, parse messages and convert to signals
            if messages:
                signals = []
                for message in messages:
                    # parse and save every signal
                    try:
                        signal = self._parse_message(message)
                    except Exception:
                        self.logger.exception("Failed to parse kafka message:"
                                              " '{0}'".format(message))
                        continue
                    signals.append(signal)

                self.notify_signals(signals)

        self.logger.debug("Exiting message loop")

    def _connect(self):
        super()._connect()
        self._consumer = SimpleConsumer(self._kafka,
                                        self._encoded_group,
                                        self._encoded_topic,
                                        fetch_size_bytes=self.fetch_size(),
                                        buffer_size=self.fetch_size(),
                                        max_buffer_size=self.fetch_size() * 8)

    def _disconnect(self):
        if self._consumer:
            self._consumer.stop()
            self._consumer = None
        super()._disconnect()

    @property
    def connected(self):
        return super().connected and self._consumer

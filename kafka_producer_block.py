import pickle

from kafka.producer import SimpleProducer

from .kafka_base_block import KafkaBase

from nio.common.discovery import Discoverable, DiscoverableType


@Discoverable(DiscoverableType.block)
class KafkaProducer(KafkaBase):

    """ A block for producing Kafka messages
    """
    def __init__(self):
        super().__init__()
        self._producer = None

    def configure(self, context):
        super().configure(context)
        self._connect()

    def stop(self):
        self._disconnect()
        super().stop()

    def process_signals(self, signals, input_id='default'):
        for signal in signals:
            try:
                if type(signal) is not bytes:
                    signal = pickle.dumps(signal)
            except:
                self._logger.exception("Signal: {0} could not be serialized".
                                       format(signal))
                return

            try:
                self._producer.send_messages(self._encoded_topic, signal)
            except:
                self._logger.exception("Failure sending signal")

    def _connect(self):
        super()._connect()
        self._producer = SimpleProducer(self._kafka)

    def _disconnect(self):
        if self._producer:
            self._producer.stop()
            self._producer = None
        super()._disconnect()

    @property
    def connected(self):
        return super().connected and self._producer

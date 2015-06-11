import pickle

from .kafka_producer_block import KafkaProducer

from nio.metadata.properties import VersionProperty
from nio.common.discovery import Discoverable, DiscoverableType


@Discoverable(DiscoverableType.block)
class KafkaBatchProducer(KafkaProducer):

    version = VersionProperty(version='0.1.0')
    """ A block for producing Kafka Batch messages
    """
    def process_signals(self, signals, input_id='default'):
        if self.connected:
            try:
                signals = pickle.dumps(signals)
            except:
                self._logger.exception(
                    "Signals: {0} could not be serialized".format(signals))
                return

            try:
                self._producer.send_messages(self._encoded_topic, signals)
            except:
                self._logger.exception("Failure sending signal")

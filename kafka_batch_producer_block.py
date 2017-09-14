import pickle

from nio.properties import VersionProperty

from .kafka_producer_block import KafkaProducer


class KafkaBatchProducer(KafkaProducer):
    """ A block for producing Kafka Batch messages """

    version = VersionProperty("1.0.1")

    def process_signals(self, signals, input_id='default'):
        if self.connected:
            try:
                signals = pickle.dumps(signals)
            except:
                self.logger.exception(
                    "Signals: {0} could not be serialized".format(signals))
                return

            try:
                self._producer.send_messages(self._encoded_topic, signals)
            except:
                self.logger.exception("Failure sending signal")

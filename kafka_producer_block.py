import json

from kafka.producer import SimpleProducer

from nio import TerminatorBlock
from nio.properties import VersionProperty

from .kafka_base_block import KafkaBase


class KafkaProducer(KafkaBase, TerminatorBlock):
    """ A block for producing Kafka messages """

    version = VersionProperty("2.0.0")

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
        msgs = []
        for signal in signals:
                signal_dict = signal.to_dict()
                signal_json = json.dumps(signal_dict)
                encoded_json = signal_json.encode('utf-8')
                msgs.append(encoded_json)
        if self.connected:
            self._producer.send_messages(self._encoded_topic, *msgs)
        else:
            self.logger.warning(
                'not connected! dropping {} messages'.format(len(msgs)))

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

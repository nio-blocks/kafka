import json
from kafka import KafkaProducer as Producer
from nio import TerminatorBlock
from nio.properties import IntProperty, ListProperty, PropertyHolder, \
    StringProperty, VersionProperty


class Servers(PropertyHolder):
    host = StringProperty(title='Host', default='[[KAFKA_HOST]]')
    port = IntProperty(title='Port', default=9092)


class KafkaProducer(TerminatorBlock):
    """ A block for producing Kafka messages """

    servers = ListProperty(Servers, title='Kafka Servers', default=[
        {
            'host': '[[KAFKA_HOST]]',
            'port': 9092,
        }
    ])
    topic = StringProperty(title='Topic')
    version = VersionProperty('3.0.0')

    def __init__(self):
        super().__init__()
        self._producer = None

    def configure(self, context):
        super().configure(context)
        servers = []
        for server in self.servers():
            host = '{}:{}'.format(server.host(), server.port())
            servers.append(host)
        self._producer = Producer(
            bootstrap_servers=servers)

    def stop(self):
        self._producer = None
        super().stop()

    def process_signals(self, signals):
        for signal in signals:
            signal_dict = signal.to_dict()
            message = json.dumps(signal_dict)
            message_bytes = message.encode('ascii')
            self._producer.send(self.topic(), message_bytes)
        self._producer.flush()

import json
from kafka import KafkaProducer as Producer
from nio import TerminatorBlock
from nio.properties import BoolProperty, IntProperty, ListProperty, \
    ObjectProperty, PropertyHolder, StringProperty, VersionProperty


class Servers(PropertyHolder):
    host = StringProperty(title='Host', default='[[KAFKA_HOST]]')
    port = IntProperty(title='Port', default=9092)


class SSL(PropertyHolder):
    ssl_check_hostname = BoolProperty(
        title='Verify Hostname',
        default=True,
        order=0)
    ssl_cafile = StringProperty(
        title='Path to Certificate Authority',
        default=None,
        allow_none=True,
        order=1)
    ssl_certfile = StringProperty(
        title='Path to Client Certificate',
        default=None,
        allow_none=True,
        order=2)
    ssl_keyfile = StringProperty(
        title='Path to Client Private Key',
        default=None,
        allow_none=True,
        order=3)


class KafkaProducer(TerminatorBlock):
    """ A block for producing Kafka messages """

    topic = StringProperty(title='Topic', order=0)
    servers = ListProperty(
        Servers,
        title='Kafka Servers',
        default=[
            {
                'host': '[[KAFKA_HOST]]',
                'port': 9092,
            }
        ],
        order=1)
    ssl = ObjectProperty(
        SSL,
        title='SSL Configuration',
        default=SSL(),
        advanced=True)
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
        # handle empty strings from SystemDesigner as None
        self._producer = Producer(
            bootstrap_servers=servers,
            ssl_check_hostname=self.ssl().ssl_check_hostname(),
            ssl_cafile=self.ssl().ssl_cafile() or None,
            ssl_certfile=self.ssl().ssl_certfile() or None,
            ssl_keyfile=self.ssl().ssl_keyfile() or None)

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

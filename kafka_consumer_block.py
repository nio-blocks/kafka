from threading import Event
from kafka import KafkaConsumer as Consumer
from nio import GeneratorBlock
from nio.properties import StringProperty, IntProperty, FloatProperty, \
    ListProperty, PropertyHolder, VersionProperty
from nio import Signal
from nio.util.threading.spawn import spawn


class Servers(PropertyHolder):
    host = StringProperty(title='Host', default='[[KAFKA_HOST]]')
    port = IntProperty(title='Port', default=9092)


class KafkaConsumer(GeneratorBlock):

    topic = StringProperty(title='Topic', order=0)
    group = StringProperty(
        title='Group',
        default=None,
        allow_none=True,
        order=1)
    servers = ListProperty(
        Servers,
        title='Kafka Servers',
        default=[
            {
                'host': '[[KAFKA_HOST]]',
                'port': 9092,
            },
        ],
        order=2)
    version = VersionProperty('3.0.0')

    def __init__(self):
        super().__init__()
        self._consumer = None
        # message loop maintenance
        self._stop_message_loop_event = None
        self._message_loop_thread = None

    def configure(self, context):
        super().configure(context)
        kwargs = {}
        kwargs['consumer_timeout_ms'] = 100
        kwargs['value_deserializer'] = self._parse_message
        if self.group() is not None:
            kwargs['group_id'] = self.group()
        servers = []
        for server in self.servers():
            servers.append('{}:{}'.format(server.host(), server.port()))
        kwargs['bootstrap_servers'] = servers
        self.logger.debug(
            'Creating Consumer for \"{}\"...'.format(self.topic()))
        self._consumer = Consumer(self.topic(), kwargs)
        self.logger.debug('Created Consumer with kwargs: {}'.format(kwargs))

    def start(self):
        super().start()
        self.logger.debug('Starting message loop...')
        self._stop_message_loop_event = Event()
        self._message_loop_thread = spawn(self._receive_messages)

    def stop(self):
        self.logger.debug('Stopping message loop...')
        self._stop_message_loop_event.set()
        self._message_loop_thread.join()
        self._message_loop_thread = None
        super().stop()

    def _parse_message(self, message):
        self.logger.debug('Got message: {}'.format(message))
        signal = dict()
        signal['key'] = message.key
        signal['offset'] = message.offset
        signal['partition'] = message.partition
        signal['topic'] = message.topic
        signal['value'] = message.value
        return Signal(signal)

    def _receive_messages(self):
        self.logger.debug('Started message loop.')
        while not self._stop_message_loop_event.is_set():
            try:
                for message in self._consumer:
                    self.notify_signals([message])
            except StopIteration:
                # consumer_timeout_ms has elapsed without a message
                continue
            except:
                self.logger.exception('Failed to fetch messages')
        self.logger.debug('Stopped message loop.')

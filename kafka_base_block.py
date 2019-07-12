from kafka import KafkaClient

from nio.block.base import Base
from nio.properties import StringProperty, IntProperty, ListProperty, \
    PropertyHolder
from nio.util.discovery import not_discoverable


class Hosts(PropertyHolder):
    host = StringProperty(title='Host', default='[[KAFKA_HOST]]')
    port = IntProperty(title='Port', default=9092)

@not_discoverable
class KafkaBase(Base):

    """ A block defining common Kafka functionality.
    Properties:
        hosts (list): location of the database
        port (int): open port served by database
        topic (str): topic name
    """
    hosts = ListProperty(Hosts, title='Kafka Hosts', default=[
        {
            'host': '[[KAFKA_HOST]]',
            'port': 9092,
        }
    ])
    topic = StringProperty(title='Topic', default='')
    ssl_cafile = StringProperty(
        title='SSL CA File',
        allow_none=True,
        advanced=True,
        order=1)
    ssl_certfile = StringProperty(
        title='SSL Certificate File',
        allow_none=True,
        advanced=True,
        order=2)
    ssl_keyfile = StringProperty(
        title='SSL Private Key File',
        allow_none=True,
        advanced=True,
        order=3)

    def __init__(self):
        super().__init__()
        self._kafka = None
        self._encoded_topic = None

    def configure(self, context):
        super().configure(context)

        if not len(self.topic()):
            raise ValueError("Topic cannot be empty")

        self._connect()

    def stop(self):
        self._disconnect()
        super().stop()

    def _connect(self):
        hosts_list = []
        for host in self.hosts():
            hosts_list.append('{}:{}'.format(host.host(), host.port()))
        self._kafka = KafkaClient(
            bootstrap_servers=hosts_list,
            # make sure that empty strings are replaced with None
            ssl_cafile=self.ssl_cafile() if self.ssl_cafile() else None,
            ssl_certfile=self.ssl_certfile() if self.ssl_certfile() else None,
            ssl_keyfile=self.ssl_keyfile() if self.ssl_keyfile() else None)
        self._encoded_topic = self.topic()

        # ensuring topic is valid
        try:
            self._kafka.ensure_topic_exists(self._encoded_topic)
        except Exception:
            self.logger.exception("Topic: {0} does not exist"
                                  .format(self.topic()))
            raise

    def _disconnect(self):
        if self._kafka:
            self._kafka.close()
            self._kafka = None

    @property
    def connected(self):
        return self._kafka

from kafka import KafkaClient

from nio.block.base import Block
from nio.properties import StringProperty, IntProperty
from nio.util.discovery import not_discoverable


@not_discoverable
class KafkaBase(Block):

    """ A block defining common Kafka functionality.
    Properties:
        host (str): location of the database
        port (int): open port served by database
        topic (str): topic name
    """
    host = StringProperty(title='Host', default='[[KAFKA_HOST]]')
    port = IntProperty(title='Port', default=9092)
    topic = StringProperty(title='Topic', default="", allow_none=False)

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
        self._kafka = KafkaClient("{0}:{1}".format(self.host(), self.port()))
        self._encoded_topic = self.topic().encode()

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

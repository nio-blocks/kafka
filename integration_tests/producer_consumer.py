from time import sleep

from nio.block.context import BlockContext
from nio.router.base import BlockRouter
from nio.signal.base import Signal
from nio.testing import AttributeDict
from nio.util import Hooks
from nio.util.process_logging import setup_logging
from nio.util import TestConfigurationProvider
from nio.service.base import hook_points

from ..kafka_consumer_block import KafkaConsumer
from ..kafka_producer_block import KafkaProducer


execution = [{"name": "senderblock",
              "receivers": ["receiverblock"]}]


if __name__ == '__main__':

    setup_logging(TestConfigurationProvider)

    router = BlockRouter()
    hooks = Hooks(hook_points)

    consumer_properties = {"host": "127.0.0.1",
                           "topic": "test",
                           "group": "my-group"}
    consumer = KafkaConsumer()
    consumer.configure(BlockContext(
        router,
        consumer_properties,
        AttributeDict(),
        hooks,
        'TestSuite',
        ''))
    consumer.start()

    producer_properties = {"host": "127.0.0.1",
                           "topic": "test"}
    producer = KafkaProducer()
    producer.configure(BlockContext(
        router,
        producer_properties,
        AttributeDict(),
        hooks,
        'TestSuite',
        ''))
    producer.start()
    signal = Signal({"attr1": 1, "attr2": "data2"})
    producer.process_signals([signal])

    sleep(0.1)

    producer.stop()
    consumer.stop()

import logging
from time import sleep
from unittest.mock import Mock
from kafka.common import OffsetAndMessage, Message

from nio.util.support.block_test_case import NIOBlockTestCase

from ..kafka_consumer_block import KafkaConsumer


class TestKafkaConsumer(NIOBlockTestCase):

    def test_connect(self):
        # asserts that connection is invoked at configure time

        blk = KafkaConsumer()
        self.assertIsNone(blk._consumer)

        blk._connect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "group": "test_group",
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk._connect.called)

    def test_empty_group(self):
        # asserts that topic value cannot be empty

        blk = KafkaConsumer()
        blk._connect = Mock()
        with self.assertRaises(ValueError):
            self.configure_block(blk, {
                "host": "127.0.0.1",
                "topic": "test_topic",
                "log_level": logging.DEBUG
            })

    def test_stop(self):
        # asserts that connect/disconnect calls are made from expected places

        blk = KafkaConsumer()
        blk._connect = Mock()
        blk._disconnect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "group": "test_group",
            "log_level": logging.DEBUG
        })
        self.assertFalse(blk.connected)
        blk.start()
        blk.stop()

        self.assertTrue(blk._disconnect.called)

    def test_parse_messages(self):
        # asserts that message parsing assumes defined data following
        # 'OffsetAndMessage' format

        blk = KafkaConsumer()
        signal = blk._parse_message(self._get_message())
        self.assertIsNotNone(signal)

    def test_receive_messages(self):
        # assert that messages are gathered and transformed to signals
        # most assertions are done in notify_signals mocked method
        blk = KafkaConsumer()
        self._blk = blk
        blk._connect = Mock(side_effect=self._test_connect)
        blk._disconnect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "group": "test_group",
            "log_level": logging.DEBUG
        })
        blk.start()
        self.assertIsNotNone(blk._message_loop_thread)
        sleep(0.1)
        blk.stop()
        # assert that message loop thread joined and was reset to None
        self.assertIsNone(blk._message_loop_thread)

    def _test_connect(self):
        # make consumer to call our get_messages
        self._blk._consumer = self
        # notify signals here
        self._blk.notify_signals = Mock(side_effect=self.notify_signals)

    def get_messages(self, count=1, block=True, timeout=0.1):
        # mocking kafka's get_messages
        sleep(timeout)
        # one kafka message back
        return [self._get_message()]

    def _get_message(self, offset="offset", magic="magic",
                     attributes="attributes", key="key", value="value"):
        message = Message(magic, attributes, key, value)
        return OffsetAndMessage(offset, message)

    def notify_signals(self, signals, output_id='default'):
        self.assertEqual(len(signals), 1)

        # assert members match simulated kafka message
        self.assertEqual(signals[0].value, "value")
        self.assertEqual(signals[0].key, "key")
        self.assertEqual(signals[0].magic, "magic")
        self.assertEqual(signals[0].attributes, "attributes")

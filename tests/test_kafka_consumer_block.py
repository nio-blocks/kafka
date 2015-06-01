import logging
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
        message = Message("magic", "attributes", "key", "value")
        blk._parse_and_notify_message(OffsetAndMessage("offset", message))
        blk.stop()

        # assert that a signal was sent
        self.assertEqual(blk._block_router.get_signals_from_block(blk), 1)

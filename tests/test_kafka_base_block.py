import logging
from unittest.mock import Mock

from nio.testing.block_test_case import NIOBlockTestCase

from ..kafka_base_block import KafkaBase


class TestKafkaBase(NIOBlockTestCase):

    def test_connect(self):
        # asserts that connection is invoked at configure time

        blk = KafkaBase()
        self.assertIsNone(blk._kafka)
        self.assertIsNone(blk._encoded_topic)

        blk._connect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk._connect.called)

    def test_empty_topic(self):
        # asserts that topic value cannot be empty

        blk = KafkaBase()
        with self.assertRaises(ValueError):
            self.configure_block(blk, {
                "host": "127.0.0.1",
                "log_level": logging.DEBUG
            })

    def test_stop(self):
        blk = KafkaBase()
        blk._connect = Mock()
        blk._disconnect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "log_level": logging.DEBUG
        })
        self.assertFalse(blk.connected)
        blk.start()
        blk.stop()

        self.assertTrue(blk._disconnect.called)

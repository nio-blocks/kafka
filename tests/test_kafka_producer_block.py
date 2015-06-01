import pickle
import logging
from unittest.mock import Mock

from nio.common.signal.base import Signal
from nio.util.support.block_test_case import NIOBlockTestCase

from ..kafka_producer_block import KafkaProducer


class TestKafkaProducer(NIOBlockTestCase):

    def test_connect(self):
        # asserts that connection is invoked at configure time

        blk = KafkaProducer()
        self.assertIsNone(blk._producer)

        blk._connect = Mock()
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": "test_topic",
            "log_level": logging.DEBUG
        })
        self.assertTrue(blk._connect.called)

    def test_stop(self):
        # asserts that connect/disconnect calls are made from expected places

        blk = KafkaProducer()
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

    def test_process_signals(self):
        # asserts that signal processing executes and message itself is
        # serialized as expected

        blk = KafkaProducer()
        blk._connect = Mock()
        blk._disconnect = Mock()
        self._topic = "test_topic"
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": self._topic,
            "log_level": logging.DEBUG
        })
        # set to receive "send_messages" calls
        blk._producer = self
        self.assertFalse(blk.connected)
        blk.start()
        self._signal_to_send = Signal({"field1": "field1_data"})
        self._signal_received = False
        blk.process_signals([self._signal_to_send])
        blk.stop()
        self.assertTrue(blk._disconnect.called)
        self.assertTrue(self._signal_received)

    def send_messages(self, topic, signal):
        self._signal_received = True
        # verify that signal is sent as bytes
        self.assertTrue(type(signal) is bytes)
        # verify topic is sent "encoded"
        self.assertEqual(topic, self._topic.encode())
        # "reverse engineer" signal and verify
        received_signal = pickle.loads(signal)
        self.assertEqual(received_signal, self._signal_to_send)

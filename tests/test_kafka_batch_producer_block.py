import pickle
import logging
from unittest.mock import Mock

from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase

from ..kafka_batch_producer_block import KafkaBatchProducer


class TestKafkaBatchProducer(NIOBlockTestCase):

    def test_process_signals(self):
        # asserts that signal processing executes and message itself is
        # serialized as expected

        blk = KafkaBatchProducer()
        blk._connect = Mock()
        blk._disconnect = Mock()
        blk._producer = Mock()
        self._topic = "test_topic"
        self.configure_block(blk, {
            "host": "127.0.0.1",
            "topic": self._topic,
            "log_level": logging.DEBUG
        })
        self.assertFalse(blk.connected)
        blk.start()
        # Trick it into thinking we're connected
        blk._kafka = True
        blk._producer.send_messages = Mock(side_effect=self.send_messages)
        self.assertTrue(blk.connected)
        self._signal_to_send = Signal({"field1": "field1_data"})
        self._signal_received = False
        blk.process_signals([self._signal_to_send])
        self.assertTrue(blk._producer.send_messages.called)
        blk.stop()
        self.assertTrue(blk._disconnect.called)
        self.assertTrue(self._signal_received)

    def send_messages(self, topic, signal):
        self._signal_received = True
        # verify that signal is sent as bytes
        self.assertTrue(isinstance(signal, bytes))
        # verify topic is sent "encoded"
        self.assertEqual(topic, self._topic.encode())
        # "reverse engineer" signal and verify
        received_signal = pickle.loads(signal)
        self.assertEqual(received_signal, self._signal_to_send)

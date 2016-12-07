import logging
import pickle
from time import sleep
from unittest.mock import Mock

from kafka.common import OffsetAndMessage, Message

from nio.signal.base import Signal
from nio.testing.block_test_case import NIOBlockTestCase

from ..kafka_batch_consumer_block import KafkaBatchConsumer


class TestKafkaBatchConsumer(NIOBlockTestCase):

    def test_receive_messages(self):
        # assert that signal messages are passed accordingly
        # most assertions are done in notify_signals mocked method
        blk = KafkaBatchConsumer()
        self._blk = blk
        blk._connect = Mock(side_effect=self._test_connect)
        blk._disconnect = Mock()
        self.configure_block(blk,   {
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
        self._signal_attrs = {"attr1": "attr1_value"}
        message_value = pickle.dumps([Signal(self._signal_attrs)])
        return [self._get_message(value=message_value)]

    def _get_message(self, offset="offset", magic="magic",
                     attributes="attributes", key="key", value="value"):
        message = Message(magic, attributes, key, value)
        return OffsetAndMessage(offset, message)

    def notify_signals(self, signals, output_id='default'):
        self.assertEqual(len(signals), 1)

        # assert members match simulated kafka message
        for key in self._signal_attrs.keys():
            self.assertEqual(getattr(signals[0], key), self._signal_attrs[key])

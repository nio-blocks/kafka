from time import sleep
from unittest.mock import patch, Mock

from kafka.common import OffsetAndMessage, Message

from nio.testing.block_test_case import NIOBlockTestCase

from ..kafka_consumer_block import KafkaConsumer


class TestKafkaConsumer(NIOBlockTestCase):

    def test_connect(self):
        # asserts that connection is invoked at configure time

        blk = KafkaConsumer()
        self.assertIsNone(blk._consumer)

        blk._connect = Mock()
        self.configure_block(blk, {
            "topic": "test_topic",
            "group": "test_group",
        })
        self.assertTrue(blk._connect.called)

    def test_empty_group(self):
        # asserts that topic value cannot be empty

        blk = KafkaConsumer()
        blk._connect = Mock()
        with self.assertRaises(ValueError):
            self.configure_block(blk, {
                "topic": "test_topic",
            })

    def test_stop(self):
        # asserts that connect/disconnect calls are made from expected places

        blk = KafkaConsumer()
        blk._connect = Mock()
        blk._disconnect = Mock()
        self.configure_block(blk, {
            "topic": "test_topic",
            "group": "test_group",
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
            "topic": "test_topic",
            "group": "test_group",
        })
        blk.start()
        self.assertIsNotNone(blk._message_loop_thread)
        sleep(0.1)
        blk.stop()
        # assert that message loop thread joined and was reset to None
        self.assertIsNone(blk._message_loop_thread)

    @patch(KafkaConsumer.__module__ + '.SimpleConsumer')
    @patch('blocks.kafka.kafka_base_block.KafkaClient')
    def test_fetch_size_configuration(self, MockClient, MockConsumer):
        # test implementation of fetch count, timeout, and max size options
        count = 42
        max_size = 8675309
        timeout = .314  # seconds

        mock_client = Mock()
        MockClient.return_value = mock_client
        mock_consumer = Mock(return_value=[self._get_message()])
        MockConsumer.return_value.get_messages = mock_consumer

        blk = KafkaConsumer()
        self.configure_block(blk, {
            "topic": "test_topic",
            "group": "test_group",
            "max_msg_count": count,
            "fetch_size": max_size,
            "timeout": timeout,
        })

        args, kwargs = MockConsumer.call_args
        self.assertEqual(
            args,
            (mock_client, b"test_group", "test_topic"))
        self.assertDictEqual(
            kwargs,
            {
                "fetch_size_bytes": max_size,
                "buffer_size": max_size,
                "max_buffer_size": max_size * 2**3,
            })

        blk.start()

        args, kwargs = blk._consumer.get_messages.call_args
        self.assertEqual(args, ())
        self.assertDictEqual(
            kwargs,
            {
                "block": False,  # static value
                "count": count,
                "timeout": timeout,
            })

        blk.stop()


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

import json
from unittest.mock import Mock, patch
from nio import Signal
from nio.testing.block_test_case import NIOBlockTestCase
from ..kafka_producer_block import KafkaProducer


class TestKafkaProducer(NIOBlockTestCase):

    @patch(KafkaProducer.__module__ + '.Producer')
    def test_producer(self, mock_producer):
        blk = KafkaProducer()
        self.assertIsNone(blk._producer)
        self.configure_block(blk, {
            'topic': 'test',
            'servers': [
                {
                    'host': 'foo',
                    'port': 123,
                },
                {
                    'host': 'bar',
                    'port': 321,
                },
            ],
        })
        # mock_producer.assert_called_once_with(
        #     bootstrap_servers=['foo:123', 'bar:321'],
        #     ssl_cafile=None,
        #     ssl_check_hostname=True)
        self.assertEqual(
            mock_producer.call_args[1]['bootstrap_servers'],
            ['foo:123', 'bar:321'])
        self.assertEqual(blk._producer, mock_producer.return_value)
        blk.start()
        blk.process_signals([
            Signal({
                'pi': 3.142,
            }),
        ])
        message_bytes = json.dumps({'pi': 3.142,}).encode('ascii')
        self.assertEqual(mock_producer.return_value.send.call_count, 1)
        self.assertEqual(
            mock_producer.return_value.send.call_args_list[0],
            (('test', message_bytes), {}))
        mock_producer.return_value.flush.assert_called_once_with()
        blk.stop()
        self.assertIsNone(blk._producer)

    @patch(KafkaProducer.__module__ + '.Producer')
    def test_ssl_options(self, mock_producer):
        blk = KafkaProducer()
        self.configure_block(blk, {
            'topic': 'test',
            'ssl': {
                'ssl_check_hostname': False,
                'ssl_cafile': '/path/to/ca',
                'ssl_certfile': '/path/to/cert',
                'ssl_keyfile': '/path/to/key'
            },
        })
        mock_producer.assert_called_once_with(
            bootstrap_servers=['[[KAFKA_HOST]]:9092'],
            ssl_cafile='/path/to/ca',
            ssl_certfile='/path/to/cert',
            ssl_keyfile='/path/to/key',
            ssl_check_hostname=False)

    @patch(KafkaProducer.__module__ + '.Producer')
    def test_special_cases_ssl_options(self, mock_producer):
        blk = KafkaProducer()
        self.configure_block(blk, {
            'topic': 'test',
            'ssl': {
                'ssl_cafile': '',  # designer may send emtpy string as None
                'ssl_certfile': '',  # designer may send emtpy string as None
                'ssl_keyfile': '',  # designer may send emtpy string as None
            },
        })
        mock_producer.assert_called_once_with(
            bootstrap_servers=['[[KAFKA_HOST]]:9092'],
            ssl_cafile=None,
            ssl_certfile=None,
            ssl_keyfile=None,
            ssl_check_hostname=True)

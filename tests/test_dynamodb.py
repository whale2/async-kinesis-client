import asyncio
import logging
import time
from unittest import TestCase

from src.async_kinesis_client.kinesis_consumer import ShardClosedException
from tests.mocks import KinesisConsumerMock

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)


class TestConsumerWithDynamoDB(TestCase):

    def setUp(self) -> None:
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        self.host_key = 'test-host-key'
        self.iterator_timestamp = None

        self.consumer_mock = KinesisConsumerMock()
        self.consumer = self.consumer_mock.get_consumer(
            stream_name='test-stream', checkpoint_table='test-table', host_key=self.host_key,
            shard_iterator_type='AT_TIMESTAMP', iterator_timestamp='1234567890')
        self.consumer.set_checkpoint_interval(0)
        self.consumer.set_reader_sleep_time(0.05)
        self.consumer.set_lock_holding_time(2)

        self.test_data = []

    def test_consumer_with_dynamodb(self):

        async def read():
            cnt = 0
            async for shard_reader in self.consumer.get_shard_readers():
                print('Got shard reader: {}'.format(shard_reader))
                try:
                    async for record in shard_reader.get_records():
                        self.test_data.append(record[0]['Data'])
                except ShardClosedException:
                    print('Got ShardClosedException; cnt={}'.format(cnt))
                    if cnt > 1:
                        # We should get second shard reader after first one gets closed
                        # However we signal mocked method to stop returning shards after that
                        self.consumer_mock.shard_closed = True
                    cnt += 1

        async def stop_test():
            await asyncio.sleep(4)
            self.consumer.stop()
            self.assertEqual(3, len(self.test_data))
            self.assertEqual('xxxx', self.test_data[0])
            self.assertTrue(self.consumer_mock.shard_closed)

            dynamo_record = self.consumer_mock.dynamodb_mock.items.get('Shard-0000')
            self.assertEqual(self.host_key, dynamo_record.get('fqdn'))
            self.assertEqual(100000000000000000000000000, dynamo_record.get('seq'))
            self.assertEqual(2, dynamo_record.get('subseq'))
            self.assertTrue(int(time.time()) <= dynamo_record.get('expires'))
            self.assertEqual('AT_SEQUENCE_NUMBER', self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))
            self.assertEqual(
                '1000000000000000000000000001', self.consumer_mock.iterator_kwargs.get('StartingSequenceNumber'))

        async def test():
            await asyncio.gather(
                asyncio.ensure_future(stop_test()),
                asyncio.ensure_future(read())
            )

        self.event_loop.run_until_complete(test())

    def test_dropping_seq(self):

        async def read():
            self.iterator_timestamp = int(time.time())
            self.consumer = self.consumer_mock.get_consumer(
                stream_name='test-stream', checkpoint_table='test-table', host_key=self.host_key,
                shard_iterator_type='AT_TIMESTAMP', iterator_timestamp=self.iterator_timestamp
            )

            async for shard_reader in self.consumer.get_shard_readers():
                print('Got shard reader: {}'.format(shard_reader))
                try:
                    async for record in shard_reader.get_records():
                        self.test_data.append(record[0]['Data'])
                except ShardClosedException:
                    print('Got ShardClosedException')
                    # We should get second shard reader after first one gets closed
                    # However we signal mocked method to stop returning shards after that
                    self.consumer_mock.shard_closed = True

        async def stop_test():
            await asyncio.sleep(1.2)
            self.consumer.stop()
            self.assertEqual('AT_TIMESTAMP', self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))
            self.assertEqual(self.iterator_timestamp, self.consumer_mock.iterator_kwargs.get('Timestamp'))
            remove_cmd_found = False
            for cmd in self.consumer_mock.dynamodb_mock.commands:
                if cmd['cmd'] != 'update_item':
                    continue
                if cmd['kwargs']['UpdateExpression'] == 'remove seq':
                    remove_cmd_found = True
                    break
            self.assertTrue(remove_cmd_found)

        async def test():
            await asyncio.gather(
                asyncio.ensure_future(stop_test()),
                asyncio.ensure_future(read())
            )

        self.event_loop.run_until_complete(test())

    def test_recover_from_dynamo(self):
        async def test():

            consumer = self.consumer_mock.get_consumer(
                stream_name='dynamo-backed-test-stream', checkpoint_table='test-table', host_key=self.host_key,
                shard_iterator_type='SPLIT_HORIZON', recover_from_dynamo=True
            )
            async for _ in consumer.get_shard_readers():
                break
            self.assertEqual('100000000000000000000000000273',
                             self.consumer_mock.iterator_kwargs.get('StartingSequenceNumber'))
            self.assertEqual('AT_SEQUENCE_NUMBER',self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))

            consumer = self.consumer_mock.get_consumer(
                stream_name='dynamo-backed-test-stream', checkpoint_table='test-table', host_key=self.host_key,
                shard_iterator_type='SPLIT_HORIZON')
            async for _ in consumer.get_shard_readers():
                break
            self.assertIsNone(self.consumer_mock.iterator_kwargs.get('StartingSequenceNumber'))
            self.assertEqual('SPLIT_HORIZON',self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))

        self.event_loop.run_until_complete(test())
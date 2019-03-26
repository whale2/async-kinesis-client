import asyncio
import copy
import logging
from unittest import TestCase
from unittest.mock import MagicMock

import aioboto3

from src.async_kinesis_client.kinesis_consumer import AsyncKinesisConsumer, ShardClosedException


# TODO: Add tests for DynamoDB

class TestConsumer(TestCase):

    def setUp(self):
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        self.seq = 0

        self.sample_record = {
            'MillisBehindLatest': 0,
            'Records': [{'Data': 'xxxx', 'SequenceNumber': ''}],
        }

        aioboto3.setup_default_session(botocore_session=MagicMock())

        client = MagicMock()
        client.get_records = asyncio.coroutine(self.mock_get_records)
        client.describe_stream = asyncio.coroutine(self.mock_describe_stream)
        client.get_shard_iterator = asyncio.coroutine(self.mock_get_shard_iterator)

        self.consumer = AsyncKinesisConsumer(stream_name='test-stream')
        self.consumer.kinesis_client = client
        self.consumer.set_lock_duraion(1)

        self.iterator_kwargs = None

        self.test_data = []
        self.shard_closed = False

        logging.basicConfig(level=logging.DEBUG)

    async def mock_get_records(self, ShardIterator):
        self.sample_record['Records'][0]['SequenceNumber'] = str(self.seq)
        self.seq += 1
        return self.sample_record

    async def mock_describe_stream(self, StreamName):

        return {
            'StreamDescription': {
                'Shards': [
                    {
                        'ShardId': 'Shard-0000'
                    }
                ]
            }
        } if not self.shard_closed else {
            'StreamDescription': {
                'Shards': []
            }
        }

    async def mock_get_shard_iterator(self, StreamName, ShardId, **kwargs):

        self.iterator_kwargs = kwargs
        return {
            'ShardIterator': {}
        }

    def test_consumer(self):

        async def read():
            cnt = 0
            async for shard_reader in self.consumer.get_shard_readers():
                try:
                    async for record in shard_reader.get_records():
                        self.test_data.append(record[0]['Data'])
                except ShardClosedException:
                    if cnt > 0:
                        # We should get second shard reader after first one gets closed
                        # However we signal mocked method to stop returning shards after that
                        self.shard_closed = True
                    cnt += 1

        async def stop_test():
            await asyncio.sleep(2)
            self.consumer.stop()
            self.assertEqual(len(self.test_data), 2)
            self.assertEqual(self.test_data[0], 'xxxx')
            self.assertTrue(self.shard_closed)

        async def test():
            await asyncio.gather(
                asyncio.ensure_future(stop_test()),
                asyncio.ensure_future(read())
            )

        self.event_loop.run_until_complete(test())

    def test_restarting_reader(self):

        # If the reader exists due to some reason, it should be restarted
        # Restarted reader should request iterator with sequence number of the last
        # checkpointed record

        async def read():

            cnt = 0
            async for shard_reader in self.consumer.get_shard_readers():
                # First iterator should be of type 'LATEST'
                if cnt == 0:
                    self.assertEqual('LATEST', self.iterator_kwargs.get('ShardIteratorType'))
                else:
                    self.assertEqual('AT_SEQUENCE_NUMBER', self.iterator_kwargs.get('ShardIteratorType'))
                    self.assertEqual(str(cnt - 1), self.iterator_kwargs.get('StartingSequenceNumber'))
                try:
                    async for record in shard_reader.get_records():
                        self.test_data.append(copy.deepcopy(record))
                except ShardClosedException:
                    if cnt > 1:
                        # We should get second shard reader after first one gets closed
                        # However we signal mocked method to stop returning shards after that
                        self.shard_closed = True
                    cnt += 1

        async def stop_test():
            await asyncio.sleep(2)
            # Stop shard reader but not the consumer
            for id, r in self.consumer.shard_readers.items():
                r.stop()
            await asyncio.sleep(1)
            # Now stop the consumer
            self.consumer.stop()
            self.assertEqual(3, self.seq)
            self.assertEqual(3, len(self.test_data))

        async def test():
            await asyncio.gather(
                asyncio.ensure_future(stop_test()),
                asyncio.ensure_future(read())
            )

        self.event_loop.run_until_complete(test())

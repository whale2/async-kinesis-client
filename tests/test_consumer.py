import asyncio
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

        self.sample_record = {
            'MillisBehindLatest': 0,
            'Records': [{'Data': 'xxxx'}],
        }

        aioboto3.setup_default_session(botocore_session=MagicMock())

        client = MagicMock()
        client.get_records = asyncio.coroutine(self.mock_get_records)
        client.describe_stream = asyncio.coroutine(self.mock_describe_stream)
        client.get_shard_iterator = asyncio.coroutine(self.mock_get_shard_iterator)

        self.consumer = AsyncKinesisConsumer(stream_name='test-stream')
        self.consumer.kinesis_client = client
        self.consumer.set_lock_duraion(1)

        self.test_data = []
        self.shard_closed = False

        logging.basicConfig(level=logging.DEBUG)

    async def mock_get_records(self, ShardIterator):
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
        return {
            'ShardIterator': {}
        }

    def test_consmuer(self):

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

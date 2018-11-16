import asyncio
import logging
from unittest import TestCase
from unittest.mock import MagicMock

import aioboto3

from src.async_kinesis_client.kinesis_producer import AsyncKinesisProducer


class TestProducer(TestCase):

    def setUp(self):
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        aioboto3.setup_default_session(botocore_session=MagicMock())

        client = MagicMock()
        client.put_record = asyncio.coroutine(self.mock_put_record)

        self.producer = AsyncKinesisProducer(stream_name='test-stream')
        self.producer.kinesis_client = client

        self.records = []
        self.shard_closed = False

        logging.basicConfig(level=logging.DEBUG)

    async def mock_put_record(self, **record):
        self.records.append(record)
        return { 'SequenceNumber': '1' }

    def test_producer(self):

        async def test():

            await self.producer.put_record({'Data': 'zzzz'})
            await self.producer.put_record({'Data': 'wwww'})
            self.assertEqual(len(self.records), 2)
            self.assertEqual(self.records[0].get('Data').get('Data'), 'zzzz')
            self.assertEqual(self.records[1].get('Data').get('Data'), 'wwww')
            self.assertEqual(self.records[1].get('SequenceNumberForOrdering'), '1')

        self.event_loop.run_until_complete(test())
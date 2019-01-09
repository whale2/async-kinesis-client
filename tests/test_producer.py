import asyncio
import logging
from unittest import TestCase
from unittest.mock import MagicMock

import aioboto3

from src.async_kinesis_client.kinesis_producer import AsyncKinesisProducer
import src.async_kinesis_client.kinesis_producer


class TestProducer(TestCase):

    def setUp(self):
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        aioboto3.setup_default_session(botocore_session=MagicMock())

        client = MagicMock()
        client.put_record = asyncio.coroutine(self.mock_put_record)
        client.put_records = asyncio.coroutine(self.mock_put_records)

        self.producer = AsyncKinesisProducer(stream_name='test-stream')
        self.producer.kinesis_client = client

        self.records = []
        self.shard_closed = False

        logging.basicConfig(level=logging.DEBUG)

    async def mock_put_record(self, **record):
        self.records.append(record)
        return {'SequenceNumber': '1'}

    async def mock_put_records(self, **records):
        self.records.extend(records['Records'])
        return [{}]

    def test_producer(self):

        async def test():
            await self.producer.put_record({'Data': 'zzzz'})
            await self.producer.put_record({'Data': 'wwww'})
            self.assertEqual(len(self.records), 2)
            self.assertEqual(self.records[0].get('Data').get('Data'), 'zzzz')
            self.assertEqual(self.records[1].get('Data').get('Data'), 'wwww')
            self.assertEqual(self.records[1].get('SequenceNumberForOrdering'), '1')

        self.event_loop.run_until_complete(test())

    def test_multiple_records(self):

        async def test():
            records = [
                {'Data': 'zzzz'},
                {'Data': 'wwww'}
            ]
            await self.producer.put_records(records=records)
            await self.producer.flush()

            self.assertEqual(len(self.records), 2)
            self.assertEqual(self.records[0].get('Data').get('Data'), 'zzzz')
            self.assertEqual(self.records[1].get('Data').get('Data'), 'wwww')

        self.event_loop.run_until_complete(test())

    def test_limits(self):

        src.async_kinesis_client.kinesis_producer.MAX_RECORDS_IN_BATCH = 3
        src.async_kinesis_client.kinesis_producer.MAX_RECORD_SIZE = 350
        src.async_kinesis_client.kinesis_producer.MAX_BATCH_SIZE = 1000

        async def test():

            # Check that 4th record triggers flush
            records = [
                {'Data': 'zzzz'},
                {'Data': 'wwww'},
                {'Data': 'qqqq'},
                {'Data': 'dddd'},

            ]
            await self.producer.put_records(records=records)

            self.assertEqual(len(self.records), 3)
            self.assertEqual(len(self.producer.record_buf), 1)

            await self.producer.flush()

            # Check that too big record raises ValueError
            records = [
                {'Data': 'looongcatislooong' * 10 }
            ]
            try:
                await self.producer.put_records(records=records)
            except ValueError:
                pass
            else:
                self.fail('ValueError not raised')

            # Check that exceeding MAX_BATCH_SIZE triggers flush
            records = [
                {'Data': 'zzzz'},
                {'Data': 'wwww'},
                {'Data': 'qqqq'}
            ]

            self.records = []
            await self.producer.put_records(records=records)

            self.assertEqual(len(self.records), 2)
            self.assertEqual(len(self.producer.record_buf), 1)

        self.event_loop.run_until_complete(test())

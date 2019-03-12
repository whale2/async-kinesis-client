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
            await self.producer.put_record({'Data': b'zzzz'})
            await self.producer.put_record({'Data': b'wwww'})
            self.assertEqual(len(self.records), 2)
            self.assertEqual(b'zzzz', self.records[0].get('Data').get('Data'))
            self.assertEqual(b'wwww', self.records[1].get('Data').get('Data'))
            self.assertEqual('1', self.records[1].get('SequenceNumberForOrdering'))

        self.event_loop.run_until_complete(test())

    def test_multiple_records(self):

        async def test():
            records = [
                {'Data': b'zzzz'},
                {'Data': b'wwww'}
            ]
            await self.producer.put_records(records=records)
            await self.producer.flush()

            self.assertEqual(len(self.records), 2)
            self.assertEqual(b'zzzz', self.records[0].get('Data'))
            self.assertEqual(b'wwww', self.records[1].get('Data'))

        self.event_loop.run_until_complete(test())

    def test_limits(self):

        src.async_kinesis_client.kinesis_producer.MAX_RECORDS_IN_BATCH = 3
        src.async_kinesis_client.kinesis_producer.MAX_RECORD_SIZE = 10

        async def test():

            # Check that 4th record triggers flush
            records = [
                {'Data': b'zzzz'},
                {'Data': b'wwww'},
                {'Data': b'qqqq'},
                {'Data': b'dddd'},

            ]
            await self.producer.put_records(records=records)

            self.assertEqual(3, len(self.records))
            self.assertEqual(1, len(self.producer.record_buf))

            await self.producer.flush()

            # Check that too big record raises ValueError
            records = [
                {'Data': ('looongcatislooong' * 10).encode() }
            ]
            try:
                await self.producer.put_records(records=records)
            except ValueError:
                pass
            else:
                self.fail('ValueError not raised')

            src.async_kinesis_client.kinesis_producer.MAX_BATCH_SIZE = 14

            # Check that exceeding MAX_BATCH_SIZE triggers flush
            records = [
                {'Data': b'zzzz'},
                {'Data': b'wwww'},
                {'Data': b'qqqq'},
                {'Data': b'dddd'}
            ]

            self.records = []
            await self.producer.put_records(records=records)

            self.assertEqual(3, len(self.records))
            self.assertEqual(1, len(self.producer.record_buf))

        self.event_loop.run_until_complete(test())

import asyncio
import logging
from unittest import TestCase

from src.async_kinesis_client.kinesis_consumer import ShardClosedException
from tests.mocks import KinesisConsumerMock

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)


class TestConsumer(TestCase):

    def setUp(self):
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        self.consumer_mock = KinesisConsumerMock()
        self.consumer = self.consumer_mock.get_consumer(stream_name='test-stream')

        self.test_data = []

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
                        self.consumer_mock.shard_closed = True
                    cnt += 1

        async def stop_test():
            await asyncio.sleep(2)
            self.consumer.stop()
            self.assertEqual(2, len(self.test_data))
            self.assertEqual('xxxx', self.test_data[0])
            self.assertTrue(self.consumer_mock.shard_closed)

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

            self.consumer.set_checkpoint_interval(0)
            cnt = 0
            async for shard_reader in self.consumer.get_shard_readers():
                # First iterator should be of type 'LATEST'
                if cnt == 0:
                    self.assertEqual('LATEST', self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))
                else:
                    self.assertEqual('AT_SEQUENCE_NUMBER', self.consumer_mock.iterator_kwargs.get('ShardIteratorType'))
                    self.assertEqual(
                        str(100000000000000000000000000000 + cnt - 1),
                        self.consumer_mock.iterator_kwargs.get('StartingSequenceNumber'))
                try:
                    async for record in shard_reader.get_records():
                        self.test_data.append(record)
                except ShardClosedException:
                    if cnt > 1:
                        # We should get second shard reader after first one gets closed
                        # However we signal mocked method to stop returning shards after that
                        self.consumer_mock.shard_closed = True
                    cnt += 1

        async def stop_test():
            await asyncio.sleep(2)
            # Stop shard reader but not the consumer
            for _id, r in self.consumer.shard_readers.items():
                r.stop()
            await asyncio.sleep(1)
            # Now stop the consumer
            self.consumer.stop()
            self.assertEqual(100000000000000000000000000003, self.consumer_mock.seq)
            self.assertEqual(3, len(self.test_data))

        async def test():
            await asyncio.gather(
                asyncio.ensure_future(stop_test()),
                asyncio.ensure_future(read())
            )

        self.event_loop.run_until_complete(test())

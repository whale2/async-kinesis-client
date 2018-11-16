import asyncio
import os
from unittest import TestCase, skip

import aioboto3
from botocore import credentials
from aiobotocore import AioSession

from src.async_kinesis_client.kinesis_consumer import AsyncKinesisConsumer

"""
Test against live AWS. Use at your own risk.
"""

class LiveTestKinesisConsumer(TestCase):

    def setUp(self):
        try:
            self.event_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.event_loop = asyncio.new_event_loop()

        working_dir = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')

        session = AioSession(profile=os.environ.get('AWS_PROFILE'))
        provider = session.get_component('credential_provider').get_provider('assume-role')
        provider.cache = credentials.JSONFileCache(working_dir)

        aioboto3.setup_default_session(botocore_session=session)

    @skip
    def testConsumer(self):

        async def test():

            async def read_records(shard_reader):
                async for records in shard_reader.get_records():
                    c = 0
                    for r in records:
                        c += 1
                        if c > 10:
                            print('{}: Read 10 records'.format(shard_reader.shard_id))
                            c = 0

            consumer = AsyncKinesisConsumer(
                stream_name=os.environ.get('AIOKINESIS_STREAM'),
                checkpoint_table=os.environ.get('AIOKINESIS_CHECKPOINT_TABLE'))

            async def interruptor(consumer):
                await asyncio.sleep(5)
                print('Stopping consumer')
                consumer.stop()

            asyncio.ensure_future(interruptor(consumer))
            async for reader in consumer.get_shard_readers():
                print('Got shard reader for shard id: {}'.format(reader.shard_id))
                asyncio.ensure_future(read_records(reader))
            print('Consumer stopped')

        self.event_loop.run_until_complete(test())

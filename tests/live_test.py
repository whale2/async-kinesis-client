import asyncio
import os
from unittest import TestCase, skip

import aioboto3
from aioboto3.aiobotocore import AioSession
from botocore import credentials

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
    # Use at your own risk
    def testConsumer(self):

        async def test():

            async def read_records(shard_reader):
                print('Shard reader for shard {}'.format(shard_reader.shard_id))
                c = 0
                try:
                    async for records in shard_reader.get_records():
                        for r in records:
                            c += 1
                            print('Record: {}'.format(r))
                            if c == 10:
                                print('{}: millis: {}'.format(shard_reader.shard_id, shard_reader.millis_behind_latest))
                                return
                except Exception as e:
                    print('Reader exited: {}'.format(e))

            consumer = AsyncKinesisConsumer(
                stream_name=os.environ.get('AIOKINESIS_STREAM'),
                checkpoint_table=os.environ.get('AIOKINESIS_CHECKPOINT_TABLE'),
                host_key=os.environ.get('AIOKINESIS_HOST_KEY')
            )
            print('Configured consumer: {}'.format(consumer))

            async def interruptor(consumer):
                await asyncio.sleep(60)
                print('Stopping consumer')
                consumer.stop()
                self.stopped = True

            self.stopped = False
            asyncio.ensure_future(interruptor(consumer))

            while not self.stopped:
                async for reader in consumer.get_shard_readers():
                    print('Got shard reader for shard id: {}'.format(reader.shard_id))
                    asyncio.ensure_future(read_records(reader))
            print('Consumer stopped')

        print('Starting live test')
        self.event_loop.run_until_complete(test())

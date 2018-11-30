import asyncio
import datetime
import os
from collections import OrderedDict
from unittest import TestCase, skip

import gc
#import objgraph

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

    #@skip
    def testConsumer(self):

        async def test():

            async def read_records(shard_reader):
                print('Shard reader for shard {}'.format(shard_reader.shard_id))
                c = 0
                try:
                    async for records in shard_reader.get_records():
                        #print('{}: Got batch of {}'.format(shard_reader.shard_id, len(records)))
                        for r in records:
                            c += 1
                            # if c % 900 == 0 and shard_reader.shard_id == 'shardId-000000000015':
                            #     print(datetime.datetime.now())
                            #     objgraph.show_growth(limit=5)
                            if c% 1000 == 0 and shard_reader.shard_id == 'shardId-000000000015':
                                print('{}: Read next 100 records'.format(shard_reader.shard_id))
                                print('{}: millis: {}'.format(shard_reader.shard_id, shard_reader.millis_behind_latest))
                                #c = 0

                                # objgraph.show_backrefs(r, max_depth=20, filename='{}-sample.png'.format(shard_reader.shard_id))
                                # objgraph.show_refs(r, max_depth=20, filename='{}-sample-ref.png'.format(shard_reader.shard_id))

                            del r
                except Exception as e:
                    print('Reader exited: {}'.format(e))

            consumer = AsyncKinesisConsumer(
                stream_name=os.environ.get('AIOKINESIS_STREAM'),
                checkpoint_table=os.environ.get('AIOKINESIS_CHECKPOINT_TABLE'),
                host_key=os.environ.get('AIOKINESIS_HOST_KEY')
            )
            print('Configured consumer: {}'.format(consumer))

            async def interruptor(consumer):
                await asyncio.sleep(12000)
                print('Stopping consumer')
                consumer.stop()
                self.stopped = True

            async def tracer():

                while not self.stopped:
                    # snapshot = tracemalloc.take_snapshot()
                    # top_stats = snapshot.statistics('lineno')
                    #
                    # print("[ Top 10 ]")
                    # for stat in top_stats[:10]:
                    #     print(stat)
                    gc.collect()
                    print(datetime.datetime.now())
                    #objgraph.show_growth(limit=30)
                    #objgraph.show_most_common_types(limit=30)
                    #print(objgraph.get_leaking_objects())
                    # roots = objgraph.get_leaking_objects()
                    # print('roots: {}'.format(len(roots)))
                    # objgraph.show_most_common_types(objects=roots)
                    # objgraph.show_refs(roots[:5], refcounts=True, filename='roots.png')
                    # #print('GC count: {}'.format(gc.get_count()))

                    def format_frame(f):
                        keys = ['f_code', 'f_lineno']
                        return OrderedDict([(k, str(getattr(f, k))) for k in keys])

                    def show_coro(c):
                        data = OrderedDict([
                            ('txt', str(c)),
                            ('type', str(type(c))),
                            ('done', c.done()),
                            ('cancelled', False),
                            ('stack', None),
                            ('exception', None),
                        ])
                        if not c.done():
                            data['stack'] = [format_frame(x) for x in c.get_stack()]
                        else:
                            if c.cancelled():
                                data['cancelled'] = True
                            else:
                                data['exception'] = str(c.exception())
                        return data

                    #print ('Running coros: {}'.format(len(asyncio.Task.all_tasks())))
                    # for t in asyncio.Task.all_tasks():
                    #     #print(show_coro(t))
                    #     print(t)
                    await asyncio.sleep(2)


            self.stopped = False
            asyncio.ensure_future(interruptor(consumer))
            asyncio.ensure_future(tracer())

            while not self.stopped:
                try:
                    async for reader in consumer.get_shard_readers():
                        print('Got shard reader for shard id: {}'.format(reader.shard_id))
                        asyncio.ensure_future(read_records(reader))
                except Exception as e:
                    print('Shit happened: {}'.format(e))
            print('Consumer stopped')

        print('Starting live test')
        self.event_loop.run_until_complete(test())

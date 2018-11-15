# async-kinesis-client
Python Kinesis Client library utilising asyncio

Based on Kinesis-Python project by Evan Borgstrom <eborgstrom@nerdwallet.com>
https://github.com/NerdWalletOSS/kinesis-python but with asyncio magic

The problem with Kinesis-Python is that all the data ends up in a single thread 
and being checkpointed from there - so despite having many processes, the client
is clogged by checkpointing. Besides, it checkpoints every single record and this is
not configurable.

This client is based on aioboto3 library and uses Python 3.6+ async methods.

Usage:

```python
import asyncio
from async_kinesis_client import AsyncKinesisConsumer

async def read_stream():
    
    # This is a coroutine that reads all records from shard
    async def read_records(shard_reader):
        async for records in shard_reader.get_records():
            for r in records:
                print('Shard: {}; Record: {}'.format(shard_reader.shard_id, r))
                
    consumer = AsyncKinesisConsumer(
                stream_name='my-stream',
                checkpoint_table='my-checkpoint-table')
    
    # consumer will yield existing shards and will continue yielding
    # new shards if re-sharding happens             
    async for shard_reader in consumer.get_shard_readers():
        print('Got shard reader for shard id: {}'.format(shard_reader.shard_id))
        asyncio.ensure_future(read_records(shard_reader)) 

asyncio.get_event_loop().run_until_complete(read_stream())

```

AsyncShardReader and AsyncKinesisConsumer can be stopped from parallel coroutine by calling stop() method,
consumer will stop all shard reader in that case.

AsyncShardReader exposes property millis_behind_latest which could be useful for determining application performance.

Producer is rather trivial:

```python
from async_kinesis_client import AsyncKinesisProducer

# ...

async def write_stream(): 
    producer = AsyncKinesisProducer(
        stream_name='my-stream',
        ordered=True
    )
    
    await producer.put_record(
        record=record, 
        partition_key=key, 
        explicit_hash_key=None
    )

```

Currently library lacks bulk put_records() method, proper tests, packaging and was not thoroughly tested for different network events.
Actually, don't use it, it's very preliminary and WIP. 
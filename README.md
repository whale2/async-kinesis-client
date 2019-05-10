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
from async_kinesis_client.kinesis_consumer import AsyncKinesisConsumer

async def read_stream():
    
    # This is a coroutine that reads all the records from a shard
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

*AsyncShardReader* and *AsyncKinesisConsumer* can be stopped from parallel coroutine by calling *stop()* method,
consumer will stop all shard readers in that case.
If you want to be notified of shard closing, catch *ShardClosedException* while reading records

*AsyncShardReader* exposes property millis_behind_latest which could be useful for determining application performance.

*AsyncKinesisConsumer* has following configuration methods:

*set_checkpoint_interval(records)* - how many records to skip before checkpointing

*set_lock_duration(time)* - how many seconds to hold the lock. Consumer would attempt to refresh the lock before that time

*set_reader_sleep_time(time)* - how long should shard reader wait (in seconds, fractions possible) if it did not receive any records from Kinesis stream

*set_checkpoint_callback(coro)* - set callback coroutine to be called before checkpointing next batch of records. Coroutine arguments: *ShardId*, *SequenceNumber*
 
Producer is rather trivial:

```python
from async_kinesis_client.kinesis_producer import AsyncKinesisProducer

# ...

async def write_stream():
    producer = AsyncKinesisProducer(
        stream_name='my-stream',
        ordered=True
    )

    await producer.put_record(
        record=b'bytes',
        partition_key='string',     # optional, if none, default time-based key is used
        explicit_hash_key='string'  # optional
    )

```

Sending multiple records at once:

```python
from async_kinesis_client.kinesis_producer import AsyncKinesisProducer

# ...

async def write_stream():
    producer = AsyncKinesisProducer(
        stream_name='my-stream',
        ordered=True
    )

    records = [
        {
            'Data': b'bytes',
            'PartitionKey': 'string',   # optional, if none, default time-based key is used
            'ExplicitHashKey': 'string' # optional
        },
        ...
    ]

    response = await producer.put_records(
        records=records
    )

    # See boto3 docs for response structure:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records
```


AWS authentication. For testing outside AWS cloud, especially when Mutil-Factor Authentication is in use I find following snippet extremely useful:
```python
import os
import aioboto3
from botocore import credentials
from aiobotocore import AioSession

    working_dir = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')
    session = AioSession(profile=os.environ.get('AWS_PROFILE'))
    provider = session.get_component('credential_provider').get_provider('assume-role')
    provider.cache = credentials.JSONFileCache(working_dir)
    aioboto3.setup_default_session(botocore_session=session)

```

This allows re-using cached session token after completing any aws command under *awsudo*, all you need is to set AWS_PROFILE environment variable.

Currently library still not tested enough for different network events.
Use it on your own risk, you've been warned.

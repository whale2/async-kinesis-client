import asyncio
import copy
import time
from unittest.mock import MagicMock

import aioboto3
from botocore.exceptions import ClientError

from src.async_kinesis_client.kinesis_consumer import AsyncKinesisConsumer
from src.async_kinesis_client.kinesis_producer import AsyncKinesisProducer


class KinesisConsumerMock:

    def __init__(self):
        self.seq = 100000000000000000000000000000

        self.sample_record = {
            'MillisBehindLatest': 0,
            'Records': [{'Data': 'xxxx', 'SequenceNumber': ''}],
        }

        self.consumer = None
        self.iterator_kwargs = None
        self.shard_closed = False
        self.dynamodb_mock = None

        aioboto3.setup_default_session(botocore_session=MagicMock())
        aioboto3.resource = self.mock_boto_resource

    def mock_boto_resource(self, resource):
        if resource == 'dynamodb':
            self.dynamodb_mock = DynamoDBMock()
            return self.dynamodb_mock

    def get_consumer(self, **consumer_kwargs):

        client = MagicMock()
        client.get_records = asyncio.coroutine(self.mock_get_records)
        client.describe_stream = asyncio.coroutine(self.mock_describe_stream)
        client.get_shard_iterator = asyncio.coroutine(self.mock_get_shard_iterator)

        self.consumer = AsyncKinesisConsumer(**consumer_kwargs)
        self.consumer.kinesis_client = client
        self.consumer.retriable_client = client
        self.consumer.set_lock_holding_time(1)

        return self.consumer

    async def mock_get_records(self, ShardIterator):
        sample_record = copy.deepcopy(self.sample_record)
        sample_record['Records'][0]['SequenceNumber'] = str(self.seq)
        self.seq += 1
        return sample_record

    async def mock_describe_stream(self, StreamName):

        return {
            'test-stream': {
                'StreamDescription': {
                    'Shards': [
                        {
                            'ShardId': 'Shard-0000'
                        }
                    ]
                }
            },
            'dynamo-backed-test-stream': {
                'StreamDescription': {
                    'Shards': [
                        {
                            'ShardId': 'Shard-XXXX'
                        }
                    ]
                }
            }
        }.get(StreamName) if not self.shard_closed else {
            'StreamDescription': {
                'Shards': []
            }
        }

    async def mock_get_shard_iterator(self, StreamName, ShardId, **kwargs):

        self.iterator_kwargs = kwargs
        return {
            'ShardIterator': {}
        }


class KinesisProducerMock:

    def __init__(self):
        self.records = []
        self.shard_closed = False
        self.producer = None

        aioboto3.setup_default_session(botocore_session=MagicMock())

    def get_producer(self):
        client = MagicMock()
        client.put_record = asyncio.coroutine(self.mock_put_record)
        client.put_records = asyncio.coroutine(self.mock_put_records)

        self.producer = AsyncKinesisProducer(stream_name='test-stream')
        self.producer.kinesis_client = client

        return self.producer

    async def mock_put_record(self, **record):
        self.records.append(record)
        return {'SequenceNumber': '1'}

    async def mock_put_records(self, **records):
        self.records.extend(records['Records'])
        return [{}]


class DynamoDBMock:

    items = {
        'Shard-XXXX': {
            'fqdn': 'test-host-key',
            'expires': int(time.time()) + 20,
            'shard': 'Shard-XXXX',
            'superseq': 100000000000000000000000000,
            'subseq': 273,
            'seq': '100000000000000000000000000273'
        }
    }
    commands = []

    def Table(self, table):
        return self

    async def update_item(self, **kwargs):
        print('Update item; kwargs={}'.format(kwargs))
        DynamoDBMock.commands.append({'cmd': 'update_item', 'kwargs': kwargs})
        shard = kwargs.get('Key', {}).get('shard')
        item = DynamoDBMock.items.get(shard)
        condition = kwargs.get('ConditionExpression', '')
        if condition.startswith('attribute_not_exists(') and item is not None:
            exception = ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'update_item')
            raise exception
        update = kwargs.get('UpdateExpression', '')

        if item is None:
            item = {}
        if update.startswith('set'):
            for attr, name in [
                    ('fqdn', ':new_fqdn'), ('expires', ':new_expires'), ('seq', ':seq'),
                            ('subseq', ':subseq'), ('superseq', ':superseq')]:
                if attr in update:
                    item[attr] = kwargs.get('ExpressionAttributeValues', {}).get(name)
            item['shard'] = shard
            DynamoDBMock.items[shard] = item

        if update.startswith('remove'):
            for attr in ['superseq']:
                if attr in update:
                    del item[attr]

        print('Updated; items: {}'.format(DynamoDBMock.items))

    async def get_item(self, **kwargs):
        print('Get item; kwargs={}'.format(kwargs))
        DynamoDBMock.commands.append({'cmd': 'get_item', 'kwargs': kwargs})
        key = kwargs.get('Key', {}).get('shard')
        if key is None:
            return None
        item = DynamoDBMock.items.get(key)
        print('Returning item: {}'.format(item))
        return {'Item': item}

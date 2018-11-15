import time
import aioboto3


class AsyncKinesisProducer:

    def __init__(self, stream_name, ordered=True):

        self.stream_name = stream_name
        self.ordered = ordered

        self.seq = '0'

        self.client = aioboto3.client('kinesis')

    async def put_record(self, record, partition_key=None, explicit_hash_key=None):

        if partition_key is None:
            partition_key = '{0}{1}'.format(time.clock(), time.time())

        kwargs = {
            'StreamName': self.stream_name,
            'Data': record,
            'PartitionKey': partition_key,
        }

        if self.ordered:
            kwargs['SequenceNumberForOrdering'] = self.seq

        if explicit_hash_key:
            kwargs['ExplicitHashKey'] = explicit_hash_key

        resp = await self.client.put_record(**kwargs)
        if self.ordered:
            self.seq = resp.get('SequenceNumber')

    # TODO: Add bulk put_records
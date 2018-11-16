import logging
import time
import aioboto3

log = logging.getLogger(__name__)


class AsyncKinesisProducer:

    def __init__(self, stream_name, ordered=True):

        self.stream_name = stream_name
        self.ordered = ordered

        self.seq = '0'

        self.kinesis_client = aioboto3.client('kinesis')
        log.debug("Configured kinesis producer for stream '%s'; ordered=%s",
                  stream_name, ordered)

    async def put_record(self, record, partition_key=None, explicit_hash_key=None):

        if partition_key is None:
            partition_key = '{0}{1}'.format(time.process_time(), time.time())

        kwargs = {
            'StreamName': self.stream_name,
            'Data': record,
            'PartitionKey': partition_key,
        }

        if self.ordered:
            kwargs['SequenceNumberForOrdering'] = self.seq

        if explicit_hash_key:
            kwargs['ExplicitHashKey'] = explicit_hash_key

        resp = await self.kinesis_client.put_record(**kwargs)
        if self.ordered:
            self.seq = resp.get('SequenceNumber')

    # TODO: Add bulk put_records
import logging
import time

import aioboto3

from .retriable_operations import RetriableKinesisProducer

log = logging.getLogger(__name__.split('.')[-2])


# Following constants are originating from here:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records
MAX_RECORDS_IN_BATCH = 500
MAX_RECORD_SIZE = 1024 * 1024           # 1 Mb
MAX_BATCH_SIZE = 5 * MAX_RECORD_SIZE    # 5 Mb


def _get_default_partition_key():
    return '{0}{1}'.format(time.process_time(), time.time())


class AsyncKinesisProducer:

    def __init__(self, stream_name, ordered=True):

        self.stream_name = stream_name
        self.ordered = ordered

        self.seq = '0'

        self.record_buf = []
        self.buf_size = 0

        client = aioboto3.client('kinesis')
        self.kinesis_client = RetriableKinesisProducer(client=client)
        log.debug("Configured kinesis producer for stream '%s'; ordered=%s",
                  stream_name, ordered)

    async def put_record(self, record, partition_key=None, explicit_hash_key=None):
        """
        Put single record into Kinesis stream
        :param record:              record to put, bytes
        :param partition_key:       partition key to determine shard; if none, time-based key is used
        :param explicit_hash_key:   hash value used to determine the shard explicitly, overriding partition key
        :return:                    response from kinesis client, see boto3 doc
        """

        if partition_key is None:
            partition_key = _get_default_partition_key()

        kwargs = {
            'StreamName': self.stream_name,
            'Data': record,
            'PartitionKey': partition_key,
        }

        if self.ordered:
            kwargs['SequenceNumberForOrdering'] = self.seq

        kwargs['PartitionKey'] = partition_key or _get_default_partition_key()
        if explicit_hash_key:
            kwargs['ExplicitHashKey'] = explicit_hash_key

        resp = await self.kinesis_client.put_record(**kwargs)
        if self.ordered:
            self.seq = resp.get('SequenceNumber')
        return resp

    async def put_records(self, records):
        """
        Put list of records into Kinesis stream
        This call is buffered until it outgrow maximum allowed sizes (500 records or 5 Mb of data including partition
        keys) or until explicitly flushed (see flush() below)

        :param records:             iterable with records to put; has following structure:
                                    records=[
                                        {
                                            'Data': b'bytes',
                                            'ExplicitHashKey': 'string',
                                            'PartitionKey': 'string'
                                        },
                                    ],
                                    If no 'PartitionKey' given, default time-based key will be used
                                    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records
                                    for details
        :return:                    Empty list if no records were flushed, list of responses from kinesis client
                                    otherwise

                                    Raises ValueError if single record exceeds 1 Mb
                                    Currently application should check for ProvisionedThroughputExceededException
                                    in response structure itself.
        """
        resp = []
        n = 1
        for datum in records:

            if len(self.record_buf) == MAX_RECORDS_IN_BATCH:
                resp.append(await self.flush())

            data = datum.get('Data')
            if not (isinstance(data, bytes) or isinstance(data, bytearray)):
                raise TypeError('Record # {} is of type {}; accepted types are "bytes" and "bytearray"'.format(
                    n, type(data)
                ))
            record_size = len(data)

            # boto3 docs say that combined size of record and partition key should not exceed 1 MB,
            # while in reality, at least with boto3==1.9.49 and botocore==1.12.49, the key size
            # is not taken into account
            if record_size > MAX_RECORD_SIZE:
                raise ValueError('Record # {} exceeded max record size of {}; size={}; record={}'.format(
                    n, MAX_RECORD_SIZE, record_size, datum))

            if datum.get('PartitionKey') is None:
                datum['PartitionKey'] = _get_default_partition_key()

            # The same applies to batch size - only record size is counted
            if self.buf_size + record_size > MAX_BATCH_SIZE:
                resp.append(await self.flush())

            self.record_buf.append(datum)
            self.buf_size += record_size
            n += 1

        return resp

    async def flush(self):

        if len(self.record_buf) == 0:
            return

        resp = await self.kinesis_client.put_records(
            Records=self.record_buf,
            StreamName=self.stream_name
        )
        self.record_buf = []
        self.buf_size = 0
        return resp

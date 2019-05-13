import asyncio
import datetime
import logging

import aioboto3

from botocore.exceptions import ClientError

from .dynamodb import DynamoDB
from .boto_exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__.split('.')[-2])


class ReaderExitException(Exception):
    pass


class ShardClosedException(Exception):
    pass


class RetryGetRecordsException(Exception):
    pass


class StoppableProcess:

    def __init__(self):
        self.sleep_task = None
        self._stop = False

    def stop(self):
        self._stop = True
        if self.sleep_task and not self.sleep_task.done():
            self.sleep_task.cancel()

    async def interruptable_sleep(self, time):
        if self._stop:
            return False
        self.sleep_task = asyncio.ensure_future(asyncio.sleep(time))
        try:
            await self.sleep_task
            return True
        except asyncio.CancelledError:
            return not self._stop


class AsyncShardReader(StoppableProcess):
    """
    Read from a specific shard, yielding records from async generator
    Remark: how long we sleep between calls to get_records -
            this follow these best practices: http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    """

    def __init__(self, shard_id, shard_iter, dynamodb, checkpoint_interval, sleep_time, consumer):

        super(AsyncShardReader, self).__init__()

        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.sleep_time = sleep_time
        self.checkpoint_interval = checkpoint_interval
        self.dynamodb = None
        self.consumer = consumer

        log.debug('Shard reader for %s starting', self.shard_id)

        if dynamodb:
            log.debug('Using checkpoint table %s, checkpointing after every %d-th record',
                      dynamodb.table_name, self.checkpoint_interval)
            self.dynamodb = dynamodb

        self.client = consumer._get_kinesis_client()
        self.retries = 0
        self.record_count = 0
        self.is_running = True
        self.millis_behind_latest = 0
        self.last_sequence_number = 0

    async def _get_records(self):
        """
        Internal get_records method. Tries to read from kinesis shard, will raise
        exception if retrying is needed or shard is closed
        :return: list of records as returned by kinesis client
        """
        if self.shard_iter is None:
            log.debug("Shard %s has been closed, exiting", self.shard_id)
            raise ShardClosedException
        try:
            resp = await self.client.get_records(ShardIterator=self.shard_iter)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in RETRY_EXCEPTIONS:
                raise RetryGetRecordsException
            elif code == 'ExpiredIteratorException':
                log.warning('Got ExpiredIteratorException, restarting shard reader')
                raise ReaderExitException
            else:
                log.error("Client error occurred while reading: %s", e)
                self.is_running = False
                raise e
        else:
            self.shard_iter = resp.get('NextShardIterator')
            self.millis_behind_latest = resp.get('MillisBehindLatest')
            return resp.get('Records')

    async def get_records(self):
        """
        Async generator for shard records
        Handles timeouts, checkpoints and skips empty records
        :return: list of records as returned by kinesis client (i.e. base64-encoded)
        """

        callback_coro = self.consumer._get_checkpoint_callback()
        while True:
            try:
                if self._stop:
                    return
                records = await self._get_records()
                if len(records) > 0:
                    yield records
                # FIXME: Could there be empty records in the list? If yes, should we filter them out?
                self.record_count += len(records)
                if self.record_count > self.checkpoint_interval:

                    if callback_coro:
                        if not await callback_coro(self.shard_id, records[-1]['SequenceNumber']):
                            raise ShardClosedException('Shard closed by application request')
                    if self.dynamodb:
                        await self.dynamodb.checkpoint(seq=records[-1]['SequenceNumber'])
                    self.last_sequence_number = records[-1]['SequenceNumber']
                    self.record_count = 0
                self.retries = 0
            except RetryGetRecordsException as e:
                sleep_time = min((
                        30,
                        (self.retries or 1) * self.sleep_time
                    ))
                if self.retries > 5:
                    log.debug("Retrying get_records (#%d %ds): %s", self.retries + 1, sleep_time, e)
                if not await self.interruptable_sleep(sleep_time):
                    return
                self.retries += 1
            except ReaderExitException:
                self.is_running = False
                self.consumer.reader_exited(self.shard_id)
            except Exception as e:
                self.is_running = False
                # Reporting exit on exceptions
                self.consumer.reader_exited(self.shard_id)
                raise e


class AsyncKinesisConsumer(StoppableProcess):
    """
    Generates a list of AsyncShardReaders adding new reader if resharding happens or reader exits
    """

    DEFAULT_SLEEP_TIME = 0.3
    DEFAULT_CHECKPOINT_INTERVAL = 100
    DEFAULT_LOCK_HOLDING_TIME = 10
    DEFAULT_FALLBACK_TIME_DELTA = 3 * 60    # seconds

    def __init__(
            self, stream_name, checkpoint_table=None, host_key=None, shard_iterator_type=None,
            iterator_timestamp=None, shard_iterators=None):
        """
        Initialize Async Kinesis Consumer
        :param stream_name:         stream name to read from
        :param checkpoint_table:    DynamoDB table for checkpointing; If not set, checkpointing is not used
        :param host_key:            Key to identify reader instance; If not set, defaults to FQDN.
        :param shard_iterator_type  Type of shard iterator, see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.get_shard_iterator
        :param iterator_timestamp   Timestamp (datetime type) for shard iterator of type 'AT_TIMESTAMP'. See link above
        :param shard_iterators      List of shard iterators; if given, consumer will read only those shards and ignore
                                    others
        """

        super(AsyncKinesisConsumer, self).__init__()

        self.stream_name = stream_name
        self.shard_iterator_type = shard_iterator_type
        self.iterator_timestamp = iterator_timestamp
        self.restricted_shard_iterators = shard_iterators

        self.kinesis_client = aioboto3.client('kinesis')

        self.checkpoint_table = checkpoint_table
        self.checkpoint_callback = None
        self.host_key = host_key

        self.shard_readers = {}
        self.dynamodb_instances = {}
        self.stream_data = None
        self.force_rescan = True

        self.checkpoint_interval = AsyncKinesisConsumer.DEFAULT_CHECKPOINT_INTERVAL
        self.lock_holding_time = AsyncKinesisConsumer.DEFAULT_LOCK_HOLDING_TIME
        self.reader_sleep_time = AsyncKinesisConsumer.DEFAULT_SLEEP_TIME
        self.fallback_time_delta = AsyncKinesisConsumer.DEFAULT_FALLBACK_TIME_DELTA

    def set_checkpoint_callback(self, callback):
        """
        Sets application callback coroutine to be called before checkpointing next batch of records
        The callback should return True if the records received from AsyncKinesisReader were
        successfully processed by application and can be checkpointed.
        The application can try to finish processing received records before returning value from this callback.
        If False value is returned, the Shard Reader will exit
        The callback is called with following arguments:
            ShardId         - Shard Id of the shard attempting checkpointing
            SequenceNumber  - Last SequenceId of the record in batch
        :param callback:
        """
        self.checkpoint_callback = callback

    def _get_checkpoint_callback(self):
        return self.checkpoint_callback

    def set_checkpoint_interval(self, interval):
        """
        Set how many records to skip between checkpointing; Zero means checkpoint on every record
        Could be set in live AsyncKinesisConsumer
        :param interval:    how many records to skip
        """
        self.checkpoint_interval = interval
        for _, reader in self.shard_readers.items():
            reader.checkpoint_interval = interval

    def set_lock_holding_time(self, lock_duration):
        self.lock_holding_time = lock_duration

    def set_reader_sleep_time(self, sleep_time):
        self.reader_sleep_time = sleep_time

    def _get_kinesis_client(self):
        return self.kinesis_client

    def stop(self):
        for shard_id, reader in self.shard_readers.items():
            log.debug('Stopping reader for shard %s', shard_id)
            reader.stop()
        super().stop()

    def reader_exited(self, shard_id):
        log.debug('Reader for shard %s reported exit', shard_id)
        # Force shard rescan
        self.force_rescan = True
        if self.sleep_task and not self.sleep_task.done():
            self.sleep_task.cancel()

    async def get_shard_readers(self):
        """
        Async generator for shard readers. Yields shard readers for every shard in stream and keeps adding
        new readers in case of re-sharding or if reader exited due to error
        """

        stream_data = {}
        while True:
            # Check if all readers are alive and kicking
            shard_readers = {}
            shards_to_restart = {}
            for shard_id, shard_reader in self.shard_readers.items():
                if not shard_reader.is_running:
                    log.debug('Reader for shard %s is not running anymore, forcing rescan', shard_id)
                    self.force_rescan = True
                    shards_to_restart[shard_id] = shard_reader.last_sequence_number
                else:
                    shard_readers[shard_id] = shard_reader
            self.shard_readers = shard_readers
            if self.force_rescan:
                log.debug("Getting description for stream '%s'", self.stream_name)
                stream_data = await self.kinesis_client.describe_stream(StreamName=self.stream_name)
                # TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

                log.debug('Stream has %d shard(s)', len(stream_data['StreamDescription']['Shards']))
                if self.restricted_shard_iterators is not None:
                    log.debug('Ignoring shards except following: {}'.format(self.restricted_shard_iterators))

            # locks have to be either acquired first time or re-acquired if some shard reader has to be restarted
            # or refreshed
            for shard_data in stream_data['StreamDescription']['Shards']:
                shard_id = shard_data['ShardId']
                if self.restricted_shard_iterators is not None and shard_id not in self.restricted_shard_iterators:
                    continue
                iterator_args = dict(ShardIteratorType='LATEST')
                # see if we can get a lock on this shard id
                dynamodb = None
                if self.checkpoint_table:
                    try:
                        dynamodb = self.dynamodb_instances.get(shard_id,
                            DynamoDB(
                                table_name=self.checkpoint_table,
                                shard_id=shard_data['ShardId'],
                                host_key=self.host_key
                            ))
                        # obtain lock if we don't have it, or refresh if we're already holding it
                        if self.dynamodb_instances.get(shard_id) is None:
                            # If iterator type is defined and not 'LATEST', we need to drop seq from DynamoDB table
                            drop_seq = self.shard_iterator_type and self.shard_iterator_type != 'LATEST'
                            shard_locked = await dynamodb.lock_shard(
                                lock_holding_time=self.lock_holding_time, drop_seq=drop_seq)
                            self.dynamodb_instances[shard_id] = dynamodb
                        else:
                            shard_locked = await self.dynamodb_instances[shard_id].refresh_lock()
                    except ClientError as e:
                        log.warning('Error while locking shard %s: %s', shard_id, e)
                    except Exception as e:
                        raise e
                    else:
                        if not shard_locked:
                            if shard_id in self.shard_readers \
                                    and not self.shard_readers[shard_id].is_running:
                                log.warning("Can't acquire lock on shard %s and ShardReader is not running",
                                            shard_data['ShardId'])
                            # Since we failed to lock the shard we just continue to the next one
                            # Hopefully the lock will expire soon and we can acquire it on the next try
                            continue
                        else:
                            iterator_args = dynamodb.get_iterator_args()

                if shard_id not in self.shard_readers or not self.shard_readers[shard_id].is_running:

                    log.debug("%s: iterator arguments: %s", shard_id, iterator_args)

                    # override shard_iterator_type if reader for this shard is being restarted
                    if shard_id in shards_to_restart:
                        if self.checkpoint_table:
                            starting_sequence_number = await dynamodb.get_last_checkpoint()
                        else:
                            # Use sequence number stored in failed reader; Kinesis wants a string, so convert it
                            starting_sequence_number = str(shards_to_restart.get(shard_id))

                        if starting_sequence_number is not None:
                            iterator_args['ShardIteratorType'] = 'AT_SEQUENCE_NUMBER'
                            iterator_args['StartingSequenceNumber'] = starting_sequence_number
                        else:
                            # Fallback to timestamp now() - fallback_time if we can't get sequence number
                            iterator_args['ShardIteratorType'] = 'AT_TIMESTAMP'
                            iterator_args['Timestamp'] = \
                                datetime.datetime.now() - datetime.timedelta(seconds=self.fallback_time_delta)

                    # override shard_iterator_type if given in constructor
                    elif self.shard_iterator_type:
                        iterator_args['ShardIteratorType'] = self.shard_iterator_type
                        if self.shard_iterator_type == 'AT_TIMESTAMP':
                            iterator_args['Timestamp'] = self.iterator_timestamp

                    log.debug("%s: final iterator arguments: %s", shard_id, iterator_args)
                    # get our iterator
                    shard_iter = await self.kinesis_client.get_shard_iterator(
                        StreamName=self.stream_name,
                        ShardId=shard_id,
                        **iterator_args
                    )
                    # create and yield ShardReader
                    shard_reader = AsyncShardReader(
                        shard_id=shard_id,
                        shard_iter=shard_iter.get('ShardIterator'),
                        dynamodb=dynamodb,
                        checkpoint_interval=self.checkpoint_interval,
                        sleep_time=self.reader_sleep_time,
                        consumer=self
                    )
                    self.shard_readers[shard_id] = shard_reader
                    yield shard_reader

            self.force_rescan = False
            # Sleep and check if re-sharding happened
            # If interruptable_sleep returned false, we were signalled to stop
            if not await self.interruptable_sleep(self.lock_holding_time * 0.8):
                return

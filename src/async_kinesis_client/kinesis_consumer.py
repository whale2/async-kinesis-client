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

        super().__init__()

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
        self.last_sequence_number = ''

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
            else:
                log.error("Client error occurred while reading: %s", e)
                raise ReaderExitException
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
                    self.last_sequence_number = records[-1]['SequenceNumber']
                    yield records

                # FIXME: Could there be empty records in the list? If yes, should we filter them out?
                self.record_count += len(records)
                if self.record_count > self.checkpoint_interval:

                    if callback_coro:
                        if not await callback_coro(self.shard_id, records[-1]['SequenceNumber']):
                            raise ShardClosedException('Shard closed by application request')
                    if self.dynamodb:
                        await self.dynamodb.checkpoint(seq=records[-1]['SequenceNumber'])
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
                log.debug("Shard %s got exception: %s", self.shard_id, e)
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
            iterator_timestamp=None, shard_iterators=None, recover_from_dynamo=False,
            iterator_sequence_number=None, custom_kinesis_client=None):
        """
        Initialize Async Kinesis Consumer
        :param stream_name:              stream name to read from
        :param checkpoint_table:         DynamoDB table for checkpointing; If not set, checkpointing is not used
        :param host_key:                 Key to identify reader instance; If not set, defaults to FQDN.
        :param shard_iterator_type:      Type of shard iterator, see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.get_shard_iterator
        :param iterator_timestamp:       Timestamp (datetime type) for shard iterator of type 'AT_TIMESTAMP'. See link above
        :param iterator_sequence_number: Sequence number for shard iterator of type 'AT_SEQUENCE_NUMBER'. See link above
        :param shard_iterators:          List of shard iterators; if given, consumer will read only those shards and ignore
                                         others
        :param recover_from_dynamo:      If True, try to recover last read sequence number from DynamoDB during the initialization
                                         If successful, shard_iterator_type will be ignored
        :param custom_kinesis_client (aiobotocore.client.Kinesis, optional): Custom kinesis client to use instead of
            the basic client instantiated in this class. Leave as None for the default behaviour.
        """

        super(AsyncKinesisConsumer, self).__init__()

        self.stream_name = stream_name
        self.shard_iterator_type = shard_iterator_type
        self.iterator_timestamp = iterator_timestamp
        self.iterator_sequence_number = iterator_sequence_number
        self.restricted_shard_iterators = shard_iterators

        if recover_from_dynamo and not checkpoint_table:
            raise RuntimeError('Can not use recover_from_dynamo without checkpoint table')
        self.recover_from_dynamodb = recover_from_dynamo

        # Allow a custom kinesis client to be passed in. This allows for setting of any additional parameters in
        # the client without needing to track them in this library.
        if custom_kinesis_client is not None:
            self.kinesis_client = custom_kinesis_client
        else:
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
            dead_readers, live_readers = self.get_shard_census()
            self.shard_readers = live_readers

            if len(dead_readers) > 0 or self.force_rescan:
                log.debug("Getting description for stream '%s' (live shard readers: %d)",
                          self.stream_name, len(live_readers))
                stream_data = await self.kinesis_client.describe_stream(StreamName=self.stream_name)
                # TODO: handle StreamStatus -- our stream might not be ready, or might be being deleted
                # But perhaps it will just throw an exception and that would be the caller's problem

                log.debug('Stream has %d shard(s)', len(stream_data['StreamDescription']['Shards']))
                if self.restricted_shard_iterators is not None:
                    log.debug('Ignoring shards except following: {}'.format(self.restricted_shard_iterators))

            # locks have to be either acquired first time or re-acquired if some shard reader has to be restarted
            # or refreshed
            for shard_data in stream_data['StreamDescription']['Shards']:
                shard_id = shard_data['ShardId']
                if self.restricted_shard_iterators is not None and shard_id not in self.restricted_shard_iterators:
                    continue
                # see if we can get a lock on this shard id
                if self.checkpoint_table is not None:
                    if not await self.get_shard_lock(shard_id):
                        continue

                # Restart lost shard readers or spawn new
                if shard_id not in self.shard_readers or shard_id in dead_readers:

                    # override shard_iterator_type if reader for this shard is being restarted
                    iterator_args = await self.get_default_iterator_args(shard_id, dead_readers.get(shard_id))
                    log.debug("%s: iterator arguments: %s", shard_id, iterator_args)

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
                        dynamodb=self.dynamodb_instances.get(shard_id),
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

    def get_shard_census(self):

        live_readers = {}
        dead_readers = {}
        for shard_id, shard_reader in self.shard_readers.items():
            try:
                if not shard_reader.is_running:
                    log.debug('Reader for shard %s is not running anymore, forcing rescan', shard_id)
                    dead_readers[shard_id] = shard_reader
                    shard_reader.stop()
                else:
                    live_readers[shard_id] = shard_reader
            except Exception as e:
                log.warning("Got exception %s while rescanning shards; Shard: %s", e, shard_id)

        return dead_readers, live_readers

    async def get_shard_lock(self, shard_id):

        try:
            dynamodb = self.dynamodb_instances.get(shard_id)
            # obtain lock if we don't have it, or refresh if we're already holding it
            if dynamodb is None:
                dynamodb = DynamoDB(
                    table=self.checkpoint_table,
                    shard_id=shard_id,
                    host_key=self.host_key
                )
                # If iterator type is defined and not 'LATEST', we need to drop seq from DynamoDB table
                drop_seq = self.shard_iterator_type and self.shard_iterator_type != 'LATEST' \
                           and not self.recover_from_dynamodb
                shard_locked = await dynamodb.lock_shard(
                    lock_holding_time=self.lock_holding_time, drop_seq=drop_seq)
                self.dynamodb_instances[shard_id] = dynamodb
            else:
                shard_locked = await dynamodb.refresh_lock()
        except ClientError as e:
            log.warning('Error while locking shard %s: %s', shard_id, e)
            return False
        except Exception as e:
            raise e
        else:
            if not shard_locked:
                if shard_id in self.shard_readers \
                        and not self.shard_readers[shard_id].is_running:
                    log.warning("Can't acquire lock on shard %s and ShardReader is not running",
                                shard_id)
                # Since we failed to lock the shard we just continue to the next one
                # Hopefully the lock will expire soon and we can acquire it on the next try
                return False
            return True

    async def get_default_iterator_args(self, shard_id, dead_shard_reader):

        # set default iterator type
        iterator_args = dict(ShardIteratorType='LATEST')
        # We can use initial sequence number if given and if we're not restarting
        starting_sequence_number = self.iterator_sequence_number if dead_shard_reader is None else None

        # see if we can get a checkpoint on this shard id
        if self.checkpoint_table and (self.recover_from_dynamodb or dead_shard_reader is not None):
            starting_sequence_number = await self.get_dynamodb_checkpoint(shard_id)
        if dead_shard_reader is not None and not self.checkpoint_table:
            # Use sequence number stored in failed reader; Kinesis wants a string, so convert it
            starting_sequence_number = str(dead_shard_reader.last_sequence_number)
            if starting_sequence_number:
                log.debug("%s: using internally saved last checkpointed seq: %s",
                          shard_id, starting_sequence_number)
            else:
                log.warning("%s: Can not get last saved checkpointed seq!", shard_id)

        # There's a sequence number to restart from
        if starting_sequence_number is not None:
            iterator_args['ShardIteratorType'] = 'AT_SEQUENCE_NUMBER'
            iterator_args['StartingSequenceNumber'] = starting_sequence_number
            iterator_args.pop('Timestamp', None)
        # There's no number and reader was created and just died
        elif dead_shard_reader is not None:
            # Fallback to timestamp now() - fallback_time if we can't get sequence number
            iterator_args['ShardIteratorType'] = 'AT_TIMESTAMP'
            timestamp = datetime.datetime.now() - datetime.timedelta(seconds=self.fallback_time_delta)
            iterator_args['Timestamp'] = timestamp
            iterator_args.pop('StartingSequenceNumber', None)
            log.warning("%s: falling back to timestamp %s", shard_id, timestamp)
        # It's a new reader
        else:
            if self.shard_iterator_type is not None:
                iterator_args['ShardIteratorType'] = self.shard_iterator_type
            if self.shard_iterator_type == 'AT_TIMESTAMP' and self.iterator_timestamp is not None:
                iterator_args['Timestamp'] = self.iterator_timestamp

        return iterator_args

    async def get_dynamodb_checkpoint(self, shard_id):

        dynamodb = self.dynamodb_instances.get(shard_id)
        log.debug("%s: retrieving last checkpointed seq from DynamoDB", shard_id)
        return await dynamodb.get_last_checkpoint()

import asyncio
import logging
import aioboto3

from botocore.exceptions import ClientError

from .dynamodb import DynamoDB, CheckpointTimeoutException
from .boto_exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


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
    Remark: how long we sleep between calls to get_records
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
                      dynamodb, self.checkpoint_interval)
            self.dynamodb = dynamodb

        self.client = consumer._get_kinesis_client()
        self.retries = 0
        self.record_count = 0
        self.is_running = True
        self.millis_behind_latest = 0

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
            if e.response.get('Error',{}).get('Code') in RETRY_EXCEPTIONS:
                raise RetryGetRecordsException
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

        while True:
            try:
                if self._stop:
                    return
                records = await self._get_records()
                if len(records) > 0:
                    yield records
                # FIXME: Could there be empty records in the list? If yes, should we filter them out?
                self.record_count += len(records)
                if self.dynamodb and self.record_count > self.checkpoint_interval:
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
    DEFAULT_LOCK_DURATION = 30

    def __init__(self, stream_name, checkpoint_table=None):

        super(AsyncKinesisConsumer, self).__init__()

        self.stream_name = stream_name
        self.kinesis_client = aioboto3.client('kinesis')

        self.checkpoint_table = checkpoint_table

        self.shard_readers = {}
        self.dynamodb_instances = {}
        self.stream_data = None
        self.force_rescan = True

        self.checkpoint_interval = AsyncKinesisConsumer.DEFAULT_CHECKPOINT_INTERVAL
        self.lock_duration = AsyncKinesisConsumer.DEFAULT_LOCK_DURATION
        self.reader_sleep_time = AsyncKinesisConsumer.DEFAULT_SLEEP_TIME

    def set_checkpoint_interval(self, interval):
        self.checkpoint_interval = interval

    def set_lock_duraion(self, lock_duration):
        self.lock_duration = lock_duration

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
            for shard_id, shard_reader in self.shard_readers.items():
                if not shard_reader.is_running:
                    log.debug('Reader for shard %s is not running anymore, forcing rescan', shard_id)
                    self.force_rescan = True
                else:
                    shard_readers[shard_id] = shard_reader
            self.shard_readers = shard_readers
            if self.force_rescan:
                log.debug("Getting description for stream '%s'", self.stream_name)
                stream_data = await self.kinesis_client.describe_stream(StreamName=self.stream_name)
                # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

                log.debug('Stream has %d shard(s)', len(stream_data['StreamDescription']['Shards']))
            # locks have to be either acquired first time or refreshed
            for shard_data in stream_data['StreamDescription']['Shards']:
                iterator_args = dict(ShardIteratorType='LATEST')
                shard_id = shard_data['ShardId']
                # see if we can get a lock on this shard id
                dynamodb = None
                if self.checkpoint_table:
                    try:
                        dynamodb = self.dynamodb_instances.get(shard_id,
                            DynamoDB(
                                table_name=self.checkpoint_table,
                                shard_id=shard_data['ShardId']))
                        shard_locked = await dynamodb.lock_shard(self.lock_duration)
                        self.dynamodb_instances[shard_id] = dynamodb
                    except CheckpointTimeoutException as e:
                        log.warning('Timeout while locking shard %s: %s', shard_id, e)
                        pass
                    except Exception as e:
                        raise e
                    else:
                        if not shard_locked:
                            if shard_id in self.shard_readers \
                                    and not self.shard_readers[shard_id].is_running:
                                log.warning("Can't acquire lock on shard %s and ShardReader is not running", shard_data['ShardId'])
                            # Since we failed to lock the shard we just continue to the next one
                            # Hopefully the lock will expire soon and we can acquire it on the next try
                            continue
                        else:
                            iterator_args = dynamodb.get_iterator_args()
                if shard_id not in self.shard_readers or not self.shard_readers[shard_id].is_running:

                    log.debug("%s iterator arguments: %s", shard_id, iterator_args)

                    # get our initial iterator
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
            if not await self.interruptable_sleep(self.lock_duration * 0.8):
                return


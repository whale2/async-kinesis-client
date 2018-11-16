import asyncio
import logging
import socket
import time

import aioboto3

from botocore.exceptions import ClientError

from .boto_exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class CheckpointTimeoutException(Exception):
    pass


class DynamoDB:
    """
    Class for checkpointing stream using async calls to DynamoDB
    Basically, state.py from kinesis-python client by Evan Borgstrom <eborgstrom@nerdwallet.com>,
    but with async calls and tailored for per-shard operations
    """

    DEFAULT_MAX_RETRIES = 5

    def __init__(self, table_name, shard_id, max_retries=DEFAULT_MAX_RETRIES):
        """
        Initalize DynamoDB
        :param table_name: DynamoDB table name
        :param shard_id: ShardId as returned by kinesis client
        :param max_retries: Max retries for communicating with DunamoDB
        """
        self.table_name = table_name
        self.shard_id = shard_id
        self.shard = {}
        self.retries = 0
        self.max_retries = max_retries
        self.dynamo_table = aioboto3.resource('dynamodb').Table(self.table_name)

    def get_iterator_args(self):
        try:
            return dict(
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=self.shard.get('seq')
            )
        except KeyError:
            return dict(
                ShardIteratorType='LATEST'
            )

    async def retry(self, coro):
        if self.retries > self.max_retries:
            raise CheckpointTimeoutException
        await asyncio.sleep(self.retries)
        self.retries += 1
        await coro

    async def checkpoint(self, seq):
        """
        Write checkpoint, so we know where in the stream we've been
        :param seq: kinesis sequence number
        """
        fqdn = socket.getfqdn()

        try:
            # update the seq attr in our item
            # ensure our fqdn still holds the lock and the new seq is bigger than what's already there
            await self.dynamo_table.update_item(
                Key={'shard': self.shard_id},
                UpdateExpression="set seq = :seq",
                ConditionExpression="fqdn = :fqdn AND (attribute_not_exists(seq) OR seq < :seq)",
                ExpressionAttributeValues={
                    ':fqdn': fqdn,
                    ':seq': seq,
                }
            )
        except ClientError as exc:
            if exc.response('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                log.warning("Throttled while trying to read lock table in Dynamo: %s", exc)
                await self.retry(self.checkpoint(seq))

            # for all other exceptions (including condition check failures) we just re-raise
            raise
        log.debug('Shard %s: checkpointed seq %s', self.shard_id, seq)

    async def lock_shard(self, lock_holding_time):
        """
        Lock shard, so no other instance will use it
        :param lock_holding_time: how long to hold the lock
        :return: True if lock was obtained, False otherwise
        """

        log.debug('Trying to lock shard %s', self.shard_id)
        dynamo_key = {'shard': self.shard_id}
        fqdn = socket.getfqdn()
        now = time.time()
        expire_time = int(now + lock_holding_time)  # dynamo doesn't support floats

        try:
            # Do a consistent read to get the current document for our shard id
            resp = await self.dynamo_table.get_item(Key=dynamo_key, ConsistentRead=True)
            self.shard = resp.get('Item')
        except KeyError:
            # if there's no Item in the resp then the document didn't exist
            pass
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                log.warning("Throttled while trying to read lock table in DynamoDB: %s", e)
                return await self.retry(self.lock_shard(lock_holding_time))

            # all other client errors just get re-raised
            raise
        else:
            if fqdn != self.shard.get('fqdn') and now < self.shard.get('expires'):
                # we don't hold the lock and it hasn't expired
                log.debug("Not starting reader for shard %s -- locked by %s until %s",
                          self.shard_id, self.shard.get('fqdn'), self.shard.get('expires'))
                return False

        try:
            # Try to acquire the lock by setting our fqdn and calculated expires.
            # We add a condition that ensures the fqdn & expires from the document we loaded hasn't changed to
            # ensure that someone else hasn't grabbed a lock first.
            await self.dynamo_table.update_item(
                Key=dynamo_key,
                UpdateExpression="set fqdn = :new_fqdn, expires = :new_expires",
                ConditionExpression="fqdn = :current_fqdn AND expires = :current_expires",
                ExpressionAttributeValues={
                    ':new_fqdn': fqdn,
                    ':new_expires': expire_time,
                    ':current_fqdn': self.shard.get('fqdn'),
                    ':current_expires': self.shard.get('expires'),
                }
            )
        except KeyError:
            # No previous lock - this occurs because we try to reference the shard info in the attr values but we don't
            # have one.  Here our condition prevents a race condition with two readers starting up and both adding a
            # lock at the same time.
            await self.dynamo_table.update_item(
                Key=dynamo_key,
                UpdateExpression="set fqdn = :new_fqdn, expires = :new_expires",
                ConditionExpression="attribute_not_exists(#shard_id)",
                ExpressionAttributeValues={
                    ':new_fqdn': fqdn,
                    ':new_expires': expire_time,
                },
                ExpressionAttributeNames={
                    # 'shard' is a reserved word in expressions so we need to use a bound name to work around it
                    '#shard_id': 'shard',
                },
                ReturnValues='ALL_NEW'
            )
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == "ConditionalCheckFailedException":
                # someone else grabbed the lock first
                return False

            if e.response('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                log.warning("Throttled while trying to write lock table in Dynamo: %s", e)
                return await self.retry(self.lock_shard(lock_holding_time))

        # we now hold the lock
        log.debug('Locked shard %s for host %s', self.shard_id, fqdn)
        return True

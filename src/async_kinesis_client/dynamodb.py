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
    Based on state.py from kinesis-python client by Evan Borgstrom <eborgstrom@nerdwallet.com>,
    but with async calls and tailored for per-shard operations
    """

    DEFAULT_MAX_RETRIES = 5

    def __init__(self, table_name, shard_id, max_retries=DEFAULT_MAX_RETRIES, host_key=None):
        """
        Initalize DynamoDB
        :param table_name:  DynamoDB table name
        :param shard_id:    ShardId as returned by kinesis client
        :param max_retries: Max retries for communicating with DynamoDB
        :param host_key:    Key to identify reader. If no key provided, defaults to FQDN
        """
        self.table_name = table_name
        self.shard_id = shard_id
        self.shard = {}
        self.retries = 0
        self.max_retries = max_retries
        self.retry_sleep_time = 1
        self.host_key = host_key or socket.getfqdn()
        self.dynamo_table = aioboto3.resource('dynamodb').Table(self.table_name)

    def get_iterator_args(self):
        if self.shard is not None and self.shard.get('seq') is not None:
            return dict(
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=self.shard.get('seq')
            )
        else:
            return dict(
                ShardIteratorType='LATEST'
            )

    async def checkpoint(self, seq):
        """
        Write checkpoint, so we know where in the stream we've been
        :param seq: kinesis sequence number
        """
        retries = self.max_retries
        while retries > 0:
            try:
                # update the seq attr in our item
                # ensure our fqdn still holds the lock and the new seq is bigger than what's already there
                await self.dynamo_table.update_item(
                    Key={'shard': self.shard_id},
                    UpdateExpression="set seq = :seq",
                    ConditionExpression="fqdn = :fqdn AND (attribute_not_exists(seq) OR seq < :seq)",
                    ExpressionAttributeValues={
                        ':fqdn': self.host_key,
                        ':seq': seq,
                    }
                )
                log.debug('Shard %s: checkpointed seq %s', self.shard_id, seq)
                return
            except ClientError as exc:
                if exc.response.get('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                    log.warning("Throttled while trying to read lock table in Dynamo: %s", exc)
                    await asyncio.sleep(self.retry_sleep_time)
                    retries -= 1
                    continue
                else:
                    # for all other exceptions (including condition check failures) we just re-raise
                    raise exc

    async def lock_shard(self, lock_holding_time, drop_seq=False):
        """
        Lock shard, so no other instance will use it
        :param lock_holding_time: how long to hold the lock
        :return: True if lock was obtained, False otherwise
        """

        log.debug('Trying to lock shard %s', self.shard_id)
        dynamo_key = {'shard': self.shard_id}
        fqdn = self.host_key
        now = time.time()
        expire_time = int(now + lock_holding_time)  # dynamo doesn't support floats

        retries = self.max_retries
        while retries > 0:
            try:
                # Do a consistent read to get the current document for our shard id
                resp = await self.dynamo_table.get_item(Key=dynamo_key, ConsistentRead=True)
                self.shard = resp.get('Item')
                break
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                    log.warning("Throttled while trying to read lock table in DynamoDB: %s", e)
                    await asyncio.sleep(self.retry_sleep_time)
                    retries -= 1
                    continue
                else:
                    # all other client errors just get re-raised
                    raise e

        # if there's no Item in the resp then the document didn't exist
        if self.shard is not None and fqdn != self.shard.get('fqdn') and now < self.shard.get('expires'):
            # we don't hold the lock and it hasn't expired
            log.debug("Not starting reader for shard %s -- locked by %s until %s",
                      self.shard_id, self.shard.get('fqdn'), self.shard.get('expires'))
            return False

        retries = self.max_retries
        while retries > 0:
            try:
                if self.shard is not None:
                    # Try to acquire the lock by setting our fqdn and calculated expires.
                    # We add a condition that ensures the fqdn & expires from the document we loaded hasn't changed to
                    # ensure that someone else hasn't grabbed a lock first.

                    # If we're restarting from some particular timestamp or seq no, drop the one that
                    # is stored in dynamoDB table
                    update_expression = "set fqdn = :new_fqdn, expires = :new_expires"
                    if drop_seq:
                        update_expression += " remove seq"

                    await self.dynamo_table.update_item(
                        Key=dynamo_key,
                        UpdateExpression=update_expression,
                        ConditionExpression="fqdn = :current_fqdn AND expires = :current_expires",
                        ExpressionAttributeValues={
                            ':new_fqdn': fqdn,
                            ':new_expires': expire_time,
                            ':current_fqdn': self.shard.get('fqdn'),
                            ':current_expires': self.shard.get('expires'),
                        }
                    )
                    break
                else:
                    # No previous lock. Here our condition prevents a race condition with two readers
                    # starting up and both adding a lock at the same time.
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
                    break
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == "ConditionalCheckFailedException":
                    # someone else grabbed the lock first
                    log.debug('Shard %s already locked by another instance', self.shard_id)
                    return False

                if e.response.get('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                    log.warning('Throttled while trying to write lock table in Dynamo: %s', e)
                    await asyncio.sleep(self.retry_sleep_time)
                    retries -= 1
                else:
                    raise e

        # we now hold the lock
        log.debug('Locked shard %s for host %s', self.shard_id, fqdn)
        return True

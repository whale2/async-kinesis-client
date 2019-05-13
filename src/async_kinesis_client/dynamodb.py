import logging
import socket
import time

import aioboto3

from botocore.exceptions import ClientError

from .retriable_operations import RetriableDynamoDB

log = logging.getLogger(__name__.split('.')[-2])


class DynamoDB:
    """
    Class for checkpointing stream using async calls to DynamoDB
    Based on state.py from kinesis-python client by Evan Borgstrom <eborgstrom@nerdwallet.com>,
    but with async calls and tailored for per-shard operations
    """

    DEFAULT_MAX_RETRIES = 5

    def __init__(self, table_name, shard_id, max_retries=DEFAULT_MAX_RETRIES, host_key=None):
        """
        Initialize DynamoDB
        :param table_name:  DynamoDB table name
        :param shard_id:    ShardId as returned by kinesis client
        :param max_retries: Max retries for communicating with DynamoDB
        :param host_key:    Key to identify reader. If no key provided, defaults to FQDN
        """
        self.table_name = table_name
        self.shard_id = shard_id
        self.shard = {}
        self.host_key = host_key or socket.getfqdn()
        self.lock_holding_time = None
        table = aioboto3.resource('dynamodb').Table(self.table_name)
        self.dynamo_table = RetriableDynamoDB(table=table, retries=max_retries, retry_sleep_time=1)

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
        Write checkpoint, so we know where in the stream we've been; also, update the lock
        :param seq: kinesis sequence number
        """
        # Convert string sequence into two decimals
        if len(seq) < 27:
            superseq = 0
            subseq = seq
        else:
            superseq = int(seq[0:27])
            subseq = int(seq[27:])
        now = int(time.time())
        # update the seq attr in our item
        # ensure our host still holds the lock and the new seq is bigger than what's already there
        try:
            await self.dynamo_table.update_item(
                Key={'shard': self.shard_id},
                UpdateExpression='set seq = :seq, subseq = :subseq',
                ConditionExpression='expires < :expires AND fqdn = :fqdn AND (attribute_not_exists(seq) OR (seq < :seq) OR (seq = :seq AND subseq < :subseq))',
                ExpressionAttributeValues={
                    ':fqdn': self.host_key,
                    ':seq': superseq,
                    ':subseq': subseq,
                    ':expires': now
                }
            )
            log.debug('Shard %s: checkpointed seq %s', self.shard_id, seq)
            return True
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                # Can't checkpoint - we either don't hold the lock or tried to checkpoint some old record
                # We let the application decide what to do next
                log.debug('Can not checkpoint seq %s for shard %s', seq, self.shard_id)
                return False
            else:
                raise e

    async def get_lock(self):
        """
        Check if we hold a valid lock for this shard
        :return:
        """
        dynamo_key = {'shard': self.shard_id}

        # Do a consistent read to get the current document for our shard id
        resp = await self.dynamo_table.get_item(Key=dynamo_key, ConsistentRead=True)
        return resp.get('Item')

    async def lock_shard(self, lock_holding_time, drop_seq=False):
        """
        Lock shard, so no other instance will use it

        :param lock_holding_time: how long to hold the lock
        :param drop_seq: remove sequence number attribute from checkpoint record
        :return: True if lock was obtained, False otherwise
        """

        log.debug('Trying to lock shard %s', self.shard_id)
        dynamo_key = {'shard': self.shard_id}
        now = time.time()
        expire_time = int(now + lock_holding_time)

        lock = await self.get_lock()
        # if there's no Item in the resp then the document didn't exist
        if lock is not None and self.host_key != lock.get('fqdn') and now < lock.get('expires'):
            # we don't hold the lock and it hasn't expired
            log.debug('Can not acquire lock for shard %s -- locked by %s until %s',
                      self.shard_id, lock.get('fqdn'), lock.get('expires'))
            return False

        try:
            if lock is not None:
                # Try to acquire the lock by setting our fqdn and calculated expires.
                # We add a condition that ensures the fqdn & expires from the document we loaded hasn't changed to
                # ensure that someone else hasn't grabbed a lock first.

                await self.dynamo_table.update_item(
                    Key=dynamo_key,
                    UpdateExpression='set fqdn = :new_fqdn, expires = :new_expires',
                    ConditionExpression='fqdn = :current_fqdn AND expires = :current_expires',
                    ExpressionAttributeValues={
                        ':new_fqdn': self.host_key,
                        ':new_expires': expire_time,
                        ':current_fqdn': lock.get('fqdn'),
                        ':current_expires': lock.get('expires'),
                    }
                )

                # If we're restarting from some particular timestamp or seq no, drop the one that
                # is stored in dynamoDB table
                if drop_seq:
                    await self.dynamo_table.update_item(
                        Key=dynamo_key,
                        UpdateExpression='remove seq',
                        ConditionExpression='fqdn = :current_fqdn AND expires = :current_expires',
                        ExpressionAttributeValues={
                            ':current_fqdn': self.host_key,
                            ':current_expires': expire_time,
                        }
                    )
            else:
                # No previous lock. Here our condition prevents a race condition with two readers
                # starting up and both adding a lock at the same time.
                await self.dynamo_table.update_item(
                    Key=dynamo_key,
                    UpdateExpression='set fqdn = :new_fqdn, expires = :new_expires',
                    ConditionExpression='attribute_not_exists(#shard_id)',
                    ExpressionAttributeValues={
                        ':new_fqdn': self.host_key,
                        ':new_expires': expire_time,
                    },
                    ExpressionAttributeNames={
                        # 'shard' is a reserved word in expressions so we need to use a bound name to work around it
                        '#shard_id': 'shard',
                    },
                    ReturnValues='ALL_NEW'
                )

                lock = {
                    'fqdn': self.host_key,
                    'expires': expire_time,
                    'shard': self.shard_id
                }
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                # someone else grabbed the lock first
                log.debug('Shard %s already locked by another instance', self.shard_id)
                return False
            else:
                raise e

        self.shard = lock
        self.lock_holding_time = lock_holding_time
        # we now hold the lock
        log.debug('Locked shard %s for host %s', self.shard_id, self.host_key)
        return True

    async def refresh_lock(self):
        """
        Refresh already held lock
        :return: True if refresh was successful, False otherwise
        """

        now = int(time.time())
        dynamo_key = {'shard': self.shard_id}
        expire_time = now + self.lock_holding_time
        try:
            await self.dynamo_table.update_item(
                Key=dynamo_key,
                UpdateExpression='set fqdn = :new_fqdn, expires = :new_expires',
                ConditionExpression='fqdn = :current_fqdn',
                ExpressionAttributeValues={
                    ':new_fqdn': self.host_key,
                    ':new_expires': expire_time,
                    ':current_fqdn': self.host_key,
                    ':current_expires': expire_time,
                }
            )
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                # someone else grabbed the lock first
                log.debug('Shard %s already locked by another instance', self.shard_id)
                return False
            else:
                raise e
        log.debug('Refreshed lock for shard %s, host %s for next %d sec',
                  self.shard_id, self.host_key, self.lock_holding_time)
        return True

    async def get_last_checkpoint(self, ignore_fqdn=False):
        """
        Get last checkpointed sequence number for shard
        :param ignore_fqdn: If true, get last checkpoint even if fqdn doesn't match
        :return: sequence number
        """

        log.debug('Getting last checkpoint for shard %s', self.shard_id)
        dynamo_key = {'shard': self.shard_id}

        # Do a consistent read to get the current document for our shard id
        resp = await self.dynamo_table.get_item(Key=dynamo_key, ConsistentRead=True)
        item = resp.get('Item')

        if item is None or (not ignore_fqdn and self.host_key != item.get('fqdn')):
            return None

        return str(item.get('seq')) + str(item.get('subseq'))

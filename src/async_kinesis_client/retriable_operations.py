import asyncio
import logging

from botocore.exceptions import ClientError

from .boto_exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__.split('.')[-2])

class RetriableAIOBotoOperation:

    def __init__(self, retries, retry_sleep_time, resource):
        self.retries = retries
        self.retry_sleep_time = retry_sleep_time
        self.resource = resource

    async def _retry(self, op, *args, **kwargs):

        retries = self.retries
        while retries > 0:
            try:
                return await op(*args, **kwargs)
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') in RETRY_EXCEPTIONS:
                    log.warning('Throttled while trying to perform operation "%s" on resource "%s": %s',
                                op, self.resource, e)
                    await asyncio.sleep(retries * self.retry_sleep_time)
                    retries -= 1
                    continue
                else:
                    # all other client errors just get re-raised
                    raise e


class RetriableDynamoDB(RetriableAIOBotoOperation):

    def __init__(self, table, retries=3, retry_sleep_time=1):
        super().__init__(retries=retries, retry_sleep_time=retry_sleep_time, resource=table)

        self.get_item_op = getattr(self.resource, 'get_item')
        self.update_item_op = getattr(self.resource, 'update_item')

    async def get_item(self, *args, **kwargs):
        return await self._retry(self.get_item_op, *args, **kwargs)

    async def update_item(self, *args, **kwargs):
        return await self._retry(self.update_item_op, *args, **kwargs)


class RetriableKinesisProducer(RetriableAIOBotoOperation):

    def __init__(self, client, retries=3, retry_sleep_time=1):
        super().__init__(retries=retries, retry_sleep_time=retry_sleep_time, resource=client)

        self.put_record_op = getattr(self.resource, 'put_record')
        self.put_records_op = getattr(self.resource, 'put_records')

    async def put_record(self, *args, **kwargs):
        return await self._retry(self.put_record_op, *args, **kwargs)

    async def put_records(self, *args, **kwargs):
        return await self._retry(self.put_records_op, *args, **kwargs)

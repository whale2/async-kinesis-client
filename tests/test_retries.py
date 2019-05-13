import asyncio
from unittest import TestCase

from botocore.exceptions import ClientError

from src.async_kinesis_client.retriable_operations import RetriableDynamoDB
from tests.mocks import DynamoDBMock


class TestDynamoRetries(TestCase):

    def setUp(self) -> None:

        table = DynamoDBMock()
        self.original_method = table.get_item
        table.get_item = self.get_item_emulating_failures

        self.dynamodb = RetriableDynamoDB(table=table)
        self.errors = 0
        self.max_errors = 3

    async def get_item_emulating_failures(self, **kwargs):
        if self.errors < self.max_errors:
            self.errors += 1
            raise ClientError({'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'get_item')
        else:
            return await self.original_method(**kwargs)

    def test_retries(self):

        async def test():
            await self.dynamodb.update_item(
                Key={'shard': 'Shard-0000'},
                UpdateExpression='set fqdn = :new_fqdn',
                ExpressionAttributeValues={':new_fqdn': 'the-new-fqdn'}
            )

            result = await self.dynamodb.get_item(Key={'shard': 'Shard-0000'})
            self.assertIsNone(result)
            self.assertEqual(3, self.errors)

            self.max_errors = 1
            result = await self.dynamodb.get_item(Key={'shard': 'Shard-0000'})
            print(result)
            self.assertEqual('the-new-fqdn', result.get('Item').get('fqdn'))
            self.assertEqual(3, self.errors)

        asyncio.get_event_loop().run_until_complete(test())

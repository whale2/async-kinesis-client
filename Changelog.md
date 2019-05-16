## 0.2.5 (2019-05-16)

- Refactor shard reader restart code
- Add some logging for restarts

## 0.2.4 (2019-05-13)

- Fix conditional expression for checkpointing

## 0.2.3 (2019-05-13)

- Drop unused parameter from DynamoDB call

## 0.2.2 (2019-05-13)

- Fix float time in refresh_lock()

## 0.2.1 (2019-05-13)

- Don't raise ConditionalCheckFailedException when trying to checkpoint a record 

## 0.2.0 (2019-05-13)

- Add shard restrictions - read only designated shards, if given
- Add retries to KinesisProducer
- Refactor code for retries

## 0.1.7 (2019-05-10)

- Fix logging

## 0.1.6 (2019-05-10)

- Fix interaction with DynamoDB
- Add handling of ExpiredIteratorException
- Add tests

## 0.1.5 (2019-03-27)

- Fix IndexError thrown out when shard has no records

## 0.1.4 (2019-03-26)

- Fix a bug when restarted shard reader reads the shard from beginning/initial timestamp

## 0.1.3 (2019-03-11)

- Fix checking record sizes
- Add checking for payload type

## 0.1.2 (2019-01-24)

- Fix checkpointing for iterator types other than LATEST 

## 0.1.1 (2019-01-23)

- Fix deprecation warning for collections
- Fix dumb errors in dynamodb.py (forgot to add .get() )

## 0.1.0 (2019-01-09)

- The 0.0.4 release should actually be 0.1.0
- Also fixed import of utils

## 0.0.4 (2019-01-09)

- Add 'put_records' method to AsyncKinesisProducer for sending multiple records at once
- Add 'iterator_type' parameter to AsyncKinesisConsumer
- Add custom checkpoint callback

## 0.0.3 (2018-12-10)

- Add `host_key` parameter to `AsyncKinesisConsumer`, for installations that change hostname frequently 
- Minor refactoring and bugfixes
- Bump aioboto3 version

## 0.0.2 (2018-11-16)

- Initial release

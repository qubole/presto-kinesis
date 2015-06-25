# Kinesis Connector

Kinesis is Amazon’s fully managed cloud-based service for real-time processing
of large, distributed data streams.

Analogous to Kafka connector, this connector allows the use of Kinesis streams as tables in Presto, such that each data-blob in kinesis stream is presented as a row in Presto.
Streams can be live: rows will appear as data is pushed into the stream, and disappear as they are dropped once their time expires. (A message is held up for 24 hours by kinesis streams).


> This connector is Read-Only connector. It can only fetch data from kinesis streams, but can not create streams or push data into the al
ready existing streams.

Please refer to the [wiki] (https://github.com/snarayananqubole/presto-kinesis/wiki/Overview) for how to install and configure the connector. This README will show how to build the connector jar file.


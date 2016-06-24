# Kinesis Connector

Kinesis is Amazon’s fully managed cloud-based service for real-time processing
of large, distributed data streams.

Analogous to Kafka connector, this connector allows the use of Kinesis
streams as tables in Presto, such that each data-blob in kinesis
stream is presented as a row in Presto.  Streams can be live: rows
will appear as data is pushed into the stream, and disappear as they
are dropped once their time expires. (A message is held up for 24
hours by kinesis streams).

This version has been updated to work with Presto 0.147.

> This connector is Read-Only connector. It can only fetch data from
kinesis streams, but can not create streams or push data into the al
ready existing streams.


# Building

    mvn clean package

This will create ``target/presto-kinesis-<version>-bundle.tar.gz``
file which contains the connector code and its dependency jars.

# Installation

You will need to augment your presto installation on coordinator and worker nodes to make sure the connector is loaded and configured properly. We will use $PRESTO_HOME to refer to the presto installation directory.

* Create a ``kinesis.properties`` file in ``$PRESTO_HOME/etc/catalog`` directory. See [Connector Configuration] (https://github.com/stitchfix/presto-kinesis/wiki/Connector-Configuration)
* Create directory ``$PRESTO_HOME/etc/kinesis`` and create a json table definition file for every presto-kinesis table. See [Table Definition] (https://github.com/stitchfix/presto-kinesis/wiki/Table-Definitions)
* Copy contents of the tarred file to ``$PRESTO_HOME/plugin/presto-kinesis`` (create it if necessary)
* Restart the Presto server to make the changes take effect



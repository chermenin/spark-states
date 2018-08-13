## Custom state store providers for Apache Spark

[![Build Status](https://travis-ci.org/chermenin/spark-states.svg?branch=master)](https://travis-ci.org/chermenin/spark-states)

There are some classes in this repository what implementing the state store functionality to keep data between micro-batches for stateful structured streaming processing using Apache Spark.

### Motivation

Out of the box, Apache Spark has only one implementation of state store providers. It's `HDFSBackedStateStoreProvider` which stores all of the data in memory, what is a very memory consuming approach. To avoid `OutOfMemory` errors this repository and custom state store providers were created.

### Usage

To use the custom state store provider for your pipelines use the following additional configuration for the submit script:

    --conf spark.sql.streaming.stateStore.providerClass="ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider"

Here is some more information about it: https://docs.databricks.com/spark/latest/structured-streaming/production.html

### Contributing

You're welcome to submit pull requests with any changes for this repository at any time. I'll be very glad to see any contributions.

### License

The standard [Apache 2.0](LICENSE) license is used for this project.

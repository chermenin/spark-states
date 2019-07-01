## Custom state store providers for Apache Spark

[![Build Status](https://travis-ci.org/chermenin/spark-states.svg?branch=master)](https://travis-ci.org/chermenin/spark-states)
[![CodeFactor](https://www.codefactor.io/repository/github/chermenin/spark-states/badge)](https://www.codefactor.io/repository/github/chermenin/spark-states)

Custom State Stores for Apache Spark to keep data between micro-batches for stateful stream processing.

### Motivation

Out of the box, Apache Spark has only one implementation of state store providers. It's `HDFSBackedStateStoreProvider` which stores all of the data in memory, what is a very memory consuming approach. To avoid `OutOfMemory` errors, this repository and custom state store providers were created.

### Usage

To use the custom state store provider for your pipelines use the following additional configuration for the submit script:

    --conf spark.sql.streaming.stateStore.providerClass="ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider"
    
Here is some more information about it: https://docs.databricks.com/spark/latest/structured-streaming/production.html

### State Timeout
    
With semantics similar to those of `GroupState`/ `FlatMapGroupWithState`, state timeout features have been built directly into the custom state store. 

Important points to note when using State Timeouts,
 
 * Timeout value is a global param across all the groups
 * Timeouts are currently based on processing time
 * The timeout will occur once 
    1) a fixed duration has elapsed after the entry's creation, or
    2) the most recent replacement (update) of its value, or
    3) its last access
 * Unlike `GroupState`, the timeout **is not** eventual as it is independent from query progress
 * Since the processing time timeout is based on the clock time, it is affected by the variations in the system clock (i.e. time zone changes, clock skew, etc.)
 * Timeout may or may not be set to strict expiration at the slight cost of memory. More info [here](https://github.com/chermenin/spark-states/issues/1).
    
To configure state timeout, additional configuration can be added,
 
    --conf spark.sql.streaming.stateStore.stateExpirySecs=5
    --conf spark.sql.streaming.stateStore.strictExpire=true
    
Other state timeout related points,
 * For no timeout, i.e. infinite state, set `spark.sql.streaming.stateStore.stateExpirySecs=-1`
 * For stateless processing, i.e. no state, set `spark.sql.streaming.stateStore.stateExpirySecs=0`

### Contributing

You're welcome to submit pull requests with any changes for this repository at any time. I'll be very glad to see any contributions.

### License

The standard [Apache 2.0](LICENSE) license is used for this project.

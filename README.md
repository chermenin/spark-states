## Custom state store providers for Apache Spark

[![Build Status](https://travis-ci.org/chermenin/spark-states.svg?branch=master)](https://travis-ci.org/chermenin/spark-states)
[![CodeFactor](https://www.codefactor.io/repository/github/chermenin/spark-states/badge)](https://www.codefactor.io/repository/github/chermenin/spark-states)
[![Maven Central](https://img.shields.io/maven-central/v/ru.chermenin/spark-states_2.12.svg)](https://search.maven.org/artifact/ru.chermenin/spark-states_2.12)

State management extensions for Apache Spark to keep data across micro-batches during stateful stream processing.

### Motivation

Out of the box, Apache Spark has only one implementation of state store providers. It's `HDFSBackedStateStoreProvider` which stores all of the data in memory, what is a very memory consuming approach. To avoid `OutOfMemory` errors, this repository and custom state store providers were created.

### Usage

To use the custom state store provider for your pipelines use the following additional configuration for the submit script/ SparkConf:

    --conf spark.sql.streaming.stateStore.providerClass="ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider"

Here is some more information about it: https://docs.databricks.com/spark/latest/structured-streaming/production.html

Alternatively, you can use the `useRocksDBStateStore()` helper method in your application while creating the SparkSession,

```
import ru.chermenin.spark.sql.execution.streaming.state.implicits._

val spark = SparkSession.builder().master(...).useRocksDBStateStore().getOrCreate()
```

Note: For the helper methods to be available, you must import the implicits as shown above.


### State Timeout
    
With semantics similar to those of `GroupState`/ `FlatMapGroupWithState`, state timeout features have been built directly into the custom state store. 

Important points to note when using State Timeouts,
 
 * Timeouts can be set differently for each streaming query. This relies on `queryName` and its `checkpointLocation`.
 * The poll trigger set on a streaming query may or may not be set to a different value than the state expiration.
 * Timeouts are currently based on processing time
 * The timeout will occur once 
    1) a fixed duration has elapsed after the entry's creation, or
    2) the most recent replacement (update) of its value, or
    3) its last access
 * Unlike `GroupState`, the timeout **is not** eventual as it is independent from query progress
 * Since the processing time timeout is based on the clock time, it is affected by the variations in the system clock (i.e. time zone changes, clock skew, etc.)
 * Timeout may or may not be set to strict expiration at the slight cost of memory. More info [here](https://github.com/chermenin/spark-states/issues/1).
    
There are 2 different ways configure state timeout:

1. Via additional configuration on SparkConf:
 
   To set a processing time timeout for all streaming queries in strict mode.
   ```
   --conf spark.sql.streaming.stateStore.stateExpirySecs=5
   --conf spark.sql.streaming.stateStore.strictExpire=true
   ```

   To configure state timeout differently for each query the above configs can be modified to,
   ```
   --conf spark.sql.streaming.stateStore.stateExpirySecs.queryName1=5
   --conf spark.sql.streaming.stateStore.stateExpirySecs.queryName2=10
       ...
       ...
   --conf spark.sql.streaming.stateStore.strictExpire=true
   ```

2. Via `stateTimeout()` helper method _(recommended way)_:

   ```
   import ru.chermenin.spark.sql.execution.streaming.state.implicits._

   val spark: SparkSession = ...
   val streamingDF: DataFrame = ...

   streamingDF.writeStream
         .format(...)
         .outputMode(...)
         .trigger(Trigger.ProcessingTime(1000L))
         .queryName("myQuery1")
         .option("checkpointLocation", "chkpntloc")
         .stateTimeout(spark.conf, expirySecs = 5)
         .start()
   
   spark.streams.awaitAnyTermination()
   ```
   
   Preferably, the `queryName` and `checkpointLocation` can be set directly via the `stateTimeout()` method, as below:
   ```
   streamingDF.writeStream
         .format(...)
         .outputMode(...)
         .trigger(Trigger.ProcessingTime(1000L))
         .stateTimeout(spark.conf, queryName="myQuery1", expirySecs = 5, checkpointLocation ="chkpntloc")
         .start()
   ```

Note: If `queryName` is invalid/ unavailable, the streaming query will be tagged as `UNNAMED` and timeout applicable will be as per the value of `spark.sql.streaming.stateStore.stateExpirySecs` (which defaults to -1, but can be overridden via SparkConf) 

Other state timeout related points (applicable on global and query level),
 * For no timeout, i.e. infinite state, set `spark.sql.streaming.stateStore.stateExpirySecs=-1`
 * For stateless processing, i.e. no state, set `spark.sql.streaming.stateStore.stateExpirySecs=0`

### Contributing

You're welcome to submit pull requests with any changes for this repository at any time. I'll be very glad to see any contributions.

### License

The standard [Apache 2.0](LICENSE) license is used for this project.

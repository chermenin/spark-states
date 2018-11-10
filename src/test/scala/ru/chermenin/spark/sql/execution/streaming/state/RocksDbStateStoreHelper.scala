package ru.chermenin.spark.sql.execution.streaming.state

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper.{newDir, rowsToStringInt, stringToRow}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.PrivateMethodTester

import scala.util.Random

object RocksDbStateStoreHelper extends PrivateMethodTester {

  val keySchema = StructType(Seq(StructField("key", StringType, nullable = true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, nullable = true)))

  val key: String = "a"
  val batchesToRetain: Int = 3

  def newStoreProvider(): RocksDbStateStoreProvider = {
    createStoreProvider(opId = Random.nextInt(), partition = 0, keySchema = keySchema, valueSchema = valueSchema)
  }

  def newStoreProvider(storeId: StateStoreId,
                       keySchema: StructType = keySchema,
                       valueSchema: StructType = valueSchema): RocksDbStateStoreProvider = {
    createStoreProvider(
      storeId.operatorId.toInt,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation,
      keySchema = keySchema,
      valueSchema = valueSchema)
  }

  def getData(provider: RocksDbStateStoreProvider, version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId)
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def createStoreProvider(opId: Int,
                          partition: Int,
                          dir: String = newDir(),
                          hadoopConf: Configuration = new Configuration,
                          sqlConf: SQLConf = new SQLConf(),
                          keySchema: StructType = keySchema,
                          valueSchema: StructType = valueSchema): RocksDbStateStoreProvider = {
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, batchesToRetain)
    val provider = new RocksDbStateStoreProvider()
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      indexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf
    )
    provider
  }

  def snapshot(version: Int): String = s"state.snapshot.$version"

  def fileExists(provider: RocksDbStateStoreProvider, version: Int): Boolean = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = snapshot(version)
    val filePath = new File(basePath.toString, fileName)
    filePath.exists
  }

  def corruptSnapshot(provider: RocksDbStateStoreProvider, version: Int): Unit = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = snapshot(version)
    new File(basePath.toString, fileName).delete()
  }

  def minSnapshotToRetain(version: Int): Int = version - batchesToRetain + 1

  def clearDB(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(clearDB)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def contains(store: StateStore, key: String): Boolean =
    store.iterator.toSeq.map(_.key).contains(stringToRow(key))

  def size(store: StateStore): Long = store.iterator.size

  def createSQLConf(stateTTLSec: Long, isStrict: Boolean): SQLConf = {
    val sqlConf: SQLConf = new SQLConf()

    sqlConf.setConfString("spark.sql.streaming.stateStore.providerClass",
      "ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")

    sqlConf.setConfString(RocksDbStateStoreProvider.STATE_EXPIRY_SECS, stateTTLSec.toString)
    sqlConf.setConfString(RocksDbStateStoreProvider.STATE_EXPIRY_STRICT_MODE, isStrict.toString)

    sqlConf
  }

}

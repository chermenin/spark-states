/*
 * Copyright 2018 Aleksandr Chermenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.chermenin.spark.sql.execution.streaming.state

import java.lang.management.ManagementFactory

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper.{newDir, rowsToStringInt, stringToRow}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.rocksdb.BackupEngine
import org.scalatest.PrivateMethodTester
import org.apache.spark.internal.Logging
import oshi.SystemInfo
import oshi.software.os.OperatingSystem

import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.mutable

object RocksDbStateStoreHelper extends PrivateMethodTester with Logging {

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
    val reloadedProvider = newStoreProvider(provider.stateStoreId.copy(storeName="getData"))
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def createStoreProvider(opId: Int,
                          partition: Int,
                          dir: String = newDir().replace('\\','/'),
                          hadoopConf: Configuration = new Configuration,
                          sqlConf: SQLConf = new SQLConf(),
                          keySchema: StructType = keySchema,
                          valueSchema: StructType = valueSchema): RocksDbStateStoreProvider = {
    hadoopConf.set("spark.sql.streaming.checkpointFileManagerClass",
      "ru.chermenin.spark.sql.execution.streaming.state.SimpleCheckpointFileManager")
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

  /*
   * Creates a new backup engine
   * BackupEngine has to be closed manually after usage
   */
  private def createBackupEngine(provider: RocksDbStateStoreProvider) = {
    val method = PrivateMethod[BackupEngine]('createBackupEngine)
    provider invokePrivate method()
  }

  def backupExists(provider: RocksDbStateStoreProvider, version: Int): Boolean = {
    val backupEngine = createBackupEngine(provider)
    val result = backupEngine.getBackupInfo.asScala.exists(_.appMetadata.toLong==version)
    backupEngine.close()
    // return
    result
  }

  def corruptSnapshot(provider: RocksDbStateStoreProvider, version: Long): Unit = {

    // delete local backup
    val backupEngine = createBackupEngine(provider)
    val backupId = backupEngine.getBackupInfo.asScala.find(_.appMetadata().toLong==version)
      .getOrElse(throw new IllegalStateException(s"Couldn't find backup for version $version"))
      .backupId
    backupEngine.deleteBackup(backupId)
    backupEngine.close()

    // remove remote
    val cleanupRemoteBackupVersionsMethod = PrivateMethod[Unit]('cleanupRemoteBackupVersions)
    provider invokePrivate cleanupRemoteBackupVersionsMethod( Seq((version,backupId)))

    logInfo( s"corrupted snapshot version $version, backupId $backupId")
  }

  def getProviderPrivateProperty[T](provider: RocksDbStateStoreProvider, property: Symbol): T = {
    val method = PrivateMethod[T](property)
    provider invokePrivate method()
  }

  def minSnapshotToRetain(version: Int): Int = version - batchesToRetain + 1

  def contains(store: StateStore, key: String): Boolean =
    store.iterator.toSeq.map(_.key).contains(stringToRow(key))

  def size(store: StateStore): Long = store.iterator.size

  def createSQLConf(stateTTLSec: Long, isStrict: Boolean): SQLConf = {
    val sqlConf: SQLConf = new SQLConf()

    sqlConf.setConf(SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.createWithDefault(""), "ru.chermenin.spark.sql.execution.streaming.state.SimpleCheckpointFileManager")
    sqlConf.setConf(SQLConf.STATE_STORE_PROVIDER_CLASS, "ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")

    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, batchesToRetain)

    sqlConf.setConfString(RocksDbStateStoreProvider.STATE_EXPIRY_SECS, stateTTLSec.toString)
    sqlConf.setConfString(RocksDbStateStoreProvider.STATE_EXPIRY_STRICT_MODE, isStrict.toString)

    sqlConf
  }

  private lazy val memoryMXBean = ManagementFactory.getMemoryMXBean
  private lazy val mBeanServer = ManagementFactory.getPlatformMBeanServer
  private lazy val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)

  /**
    * get memory from jvm view
    */
  def getJavaMemoryUsage: Map[String,Long] = {
    val heap = memoryMXBean.getHeapMemoryUsage
    val nonHeap = memoryMXBean.getNonHeapMemoryUsage
    Map(
      "heapMemoryUsed" -> heap.getUsed,
      "heapMemoryCommitted" -> heap.getCommitted,
      "heapMemoryMax" -> heap.getMax,
      "nonheapMemoryUsed" -> nonHeap.getUsed,
      "nonheapMemoryCommitted" -> nonHeap.getCommitted,
      "nonheapMemoryMax" -> nonHeap.getMax
    )
  }

  /**
    * get memory of process from OS view for Windows
    */
  lazy val osInfo: OperatingSystem = new SystemInfo().getOperatingSystem()
  def getWindowsProcessMemoryUsage : Long = {
    osInfo.getProcess(osInfo.getProcessId).getResidentSetSize
  }

  def getLinuxProcessMemoryUsage: Map[String,Long] = {
    val commandString = s"ps -p $pid u"
    val cmd = Array("/bin/sh", "-c", commandString)
    val p = Runtime.getRuntime.exec(cmd)
    val result = scala.io.Source.fromInputStream(p.getInputStream)
    val resultParsedLines = result.getLines().toSeq.map(_.toLowerCase.split(" ").filter(!_.isEmpty))
    val resultParsed = resultParsedLines.head.zip(resultParsedLines(1)).toMap
    val vsz = resultParsed("vsz").toLong
    val rss = resultParsed("rss").toLong
    Map(
      "linuxMemoryVsz" -> vsz*1024,
      "linuxMemoryRss" -> rss*1024
    )
  }
}

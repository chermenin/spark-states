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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper.{newDir, rowsToStringInt, stringToRow}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.rocksdb.BackupEngine
import org.scalatest.PrivateMethodTester

import scala.util.Random
import scala.collection.JavaConverters._

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

  def backupExists(provider: RocksDbStateStoreProvider, version: Int): Boolean = {
    val method = PrivateMethod[BackupEngine]('backupEngine)
    val backupEngine = provider invokePrivate method()
    backupEngine.getBackupInfo.asScala.exists(_.appMetadata.toLong==version)
  }

  def corruptSnapshot(provider: RocksDbStateStoreProvider, version: Int): Unit = {
    val method = PrivateMethod[BackupEngine]('backupEngine)
    val backupEngine = provider invokePrivate method()
    val backupId = backupEngine.getBackupInfo.asScala.find(_.appMetadata().toLong==version)
      .getOrElse(throw new IllegalStateException(s"Couldn't find backup for version $version"))
      .backupId
    backupEngine.deleteBackup(backupId)
  }

  def minSnapshotToRetain(version: Int): Int = version - batchesToRetain + 1

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

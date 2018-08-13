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
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId, StateStoreTestsHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}
import StateStoreTestsHelper._

import scala.util.Random

class RocksDbStateStoreProviderSuite extends FunSuite with BeforeAndAfter with PrivateMethodTester {

  val keySchema = StructType(Seq(StructField("key", StringType, nullable = true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, nullable = true)))

  val key: String = "a"
  val batchesToRetain: Int = 3

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  test("Snapshotting") {
    val provider = createStoreProvider(opId = Random.nextInt, partition = 0)

    var currentVersion = 0

    def updateVersionTo(targetVersion: Int): Unit = {
      for (i <- currentVersion + 1 to targetVersion) {
        val store = provider.getStore(currentVersion)
        put(store, key, i)
        store.commit()
        currentVersion += 1
      }
      require(currentVersion === targetVersion)
    }

    updateVersionTo(2)
    assert(getData(provider) === Set(key -> 2))

    assert(fileExists(provider, 1))
    assert(fileExists(provider, 2))

    def verifySnapshot(version: Int): Unit = {
      updateVersionTo(version)
      provider.doMaintenance()
      require(getData(provider) === Set(key -> version), "store not updated correctly")

      val snapshotVersion = (0 to version).filter(version => fileExists(provider, version)).min
      assert(snapshotVersion >= minSnapshotToRetain(version), "no snapshot files cleaned up")

      assert(
        getData(provider, snapshotVersion) === Set(key -> snapshotVersion),
        "cleaning messed up the data of the snapshotted version"
      )

      assert(
        getData(provider) === Set(key -> version),
        "cleaning messed up the data of the final version"
      )
    }

    verifySnapshot(version = 6)
    verifySnapshot(version = 20)
  }

  test("Cleaning up") {
    val provider = createStoreProvider(opId = Random.nextInt, partition = 0)
    val maxVersion = 20

    for (i <- 1 to maxVersion) {
      val store = provider.getStore(i - 1)
      put(store, key, i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    require(rowsToSet(provider.latestIterator()) === Set(key -> maxVersion), "store not updated correctly")
    for (version <- 1 until minSnapshotToRetain(maxVersion)) {
      assert(!fileExists(provider, version)) // first snapshots should be deleted
    }

    // last couple of versions should be retrievable
    for (version <- minSnapshotToRetain(maxVersion) to maxVersion) {
      assert(getData(provider, version) === Set(key -> version))
    }
  }

  test("Corrupted snapshots") {
    val provider = createStoreProvider(opId = Random.nextInt, partition = 0)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, key, i)
      store.commit()
    }

    // clean up
    provider.doMaintenance()

    val snapshotVersion = (0 to 10).filter(version => fileExists(provider, version)).max
    assert(snapshotVersion === 6)

    // Corrupt snapshot file
    assert(getData(provider, snapshotVersion) === Set(key -> snapshotVersion))
    corruptSnapshot(provider, snapshotVersion)

    // Load data from previous correct snapshot
    assert(getData(provider, snapshotVersion) === Set(key -> (snapshotVersion - 1)))

    // Do cleanup and corrupt some more snapshots
    corruptSnapshot(provider, snapshotVersion - 1)
    corruptSnapshot(provider, snapshotVersion - 2)

    // If no correct snapshots, create empty state
    assert(getData(provider, snapshotVersion) === Set())
  }

  test("Reports metrics") {
    val provider = newStoreProvider()
    val store = provider.getStore(0)
    val noDataMemoryUsed = store.metrics.memoryUsedBytes
    put(store, key, 1)
    store.commit()
    assert(store.metrics.memoryUsedBytes > noDataMemoryUsed)
  }

  test("StateStore.get") {
    val dir = newDir()
    val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
    val storeConf = StateStoreConf.empty
    val hadoopConf = new Configuration()

    // Verify that trying to get incorrect versions throw errors
    intercept[IllegalArgumentException] {
      StateStore.get(
        storeId, keySchema, valueSchema, None, -1, storeConf, hadoopConf)
    }
    assert(!StateStore.isLoaded(storeId)) // version -1 should not attempt to load the store

    intercept[IllegalStateException] {
      StateStore.get(
        storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
    }

    // Increase version of the store and try to get again
    val store0 = StateStore.get(
      storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
    assert(store0.version === 0)
    put(store0, key, 1)
    store0.commit()

    val store1 = StateStore.get(
      storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
    assert(StateStore.isLoaded(storeId))
    assert(store1.version === 1)
    assert(rowsToSet(store1.iterator()) === Set(key -> 1))

    // Verify that you can also load older version
    val store0reloaded = StateStore.get(
      storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
    assert(store0reloaded.version === 0)
    assert(rowsToSet(store0reloaded.iterator()) === Set.empty)

    // Verify that you can remove the store and still reload and use it
    StateStore.unload(storeId)
    assert(!StateStore.isLoaded(storeId))

    val store1reloaded = StateStore.get(
      storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
    assert(StateStore.isLoaded(storeId))
    assert(store1reloaded.version === 1)
    put(store1reloaded, key, 2)
    assert(store1reloaded.commit() === 2)
    assert(rowsToSet(store1reloaded.iterator()) === Set(key -> 2))
  }

  def newStoreProvider(): RocksDbStateStoreProvider = {
    createStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  def newStoreProvider(storeId: StateStoreId): RocksDbStateStoreProvider = {
    createStoreProvider(storeId.operatorId.toInt, storeId.partitionId, dir = storeId.checkpointRootLocation)
  }

  def getData(provider: RocksDbStateStoreProvider, version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId)
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def createStoreProvider(opId: Int, partition: Int, dir: String = newDir(),
                          hadoopConf: Configuration = new Configuration): RocksDbStateStoreProvider = {
    val sqlConf = new SQLConf()
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
}

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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper._
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.scalatest.{BeforeAndAfter, FunSuite}
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreHelper._

import scala.util.Random

class RocksDbStateStoreProviderSuite extends FunSuite with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  test("Versioning") {
    val provider = createStoreProvider(opId = math.abs(Random.nextInt), partition = 0)

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

    assert(backupExists(provider, 1))
    assert(backupExists(provider, 2))

    def verifySnapshot(version: Int): Unit = {
      updateVersionTo(version)
      provider.doMaintenance()
      require(getData(provider) === Set(key -> version), "store not updated correctly")

      val snapshotVersion = (0 to version).filter(version => backupExists(provider, version)).min
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
    val provider = createStoreProvider(opId = math.abs(Random.nextInt), partition = 1)
    val maxVersion = 20

    for (i <- 1 to maxVersion) {
      val store = provider.getStore(i - 1)
      put(store, key, i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    require(rowsToSet(provider.latestIterator()) === Set(key -> maxVersion), "store not updated correctly")
    for (version <- 1 until minSnapshotToRetain(maxVersion)) {
      assert(!backupExists(provider, version)) // first snapshots should be deleted
    }

    // last couple of versions should be retrievable
    for (version <- minSnapshotToRetain(maxVersion) to maxVersion) {
      assert(getData(provider, version) === Set(key -> version))
    }
  }

  test("Corrupted snapshots") {
    val provider = createStoreProvider(opId = math.abs(Random.nextInt), partition = 2)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, key, i)
      store.commit()
    }

    // clean up
    provider.doMaintenance()

    val snapshotVersion = (0 to 10).filter(version => backupExists(provider, version)).max
    assert(snapshotVersion === 6)

    // Corrupt snapshot file
    assert(getData(provider, snapshotVersion) === Set(key -> snapshotVersion))
    corruptSnapshot(provider, snapshotVersion)

    // Load data from previous correct snapshot
    assert(getData(provider, snapshotVersion - 1) === Set(key -> (snapshotVersion - 1)))

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

}

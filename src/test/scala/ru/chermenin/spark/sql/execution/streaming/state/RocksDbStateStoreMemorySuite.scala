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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper._
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.scalatest.{BeforeAndAfter, FunSuite}
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreHelper._

import scala.util.Random

class RocksDbStateStoreMemorySuite extends FunSuite with BeforeAndAfter with Logging {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  test("Memory Usage") {
    val provider = createStoreProvider(opId = math.abs(Random.nextInt), partition = 0)
    var currentVersion = 0

    def logMemory(comment:String = "") = {
      val osMem = RocksDbStateStoreHelper.getMemoryUtilization
      logInfo(s"memory $comment: "+osMem("nonheapMemoryUsed"))
      osMem("nonheapMemoryUsed")
    }
    def simulateVersion(): Unit = {
      val store = provider.getStore(currentVersion)
      (1 to 100000).foreach { i =>
        val k = Random.nextInt.toString
        val v = get(store, k)
        put(store, k, i)
      }
      store.commit()
      currentVersion += 1
      if (currentVersion % 3 == 0) provider.doMaintenance()
      logMemory()
    }

    val baseMem = logMemory( "    ")
    (1 to 10).foreach( _ => simulateVersion())
    val finalMem = logMemory()
    assert(finalMem<=baseMem*1.1)
  }
}

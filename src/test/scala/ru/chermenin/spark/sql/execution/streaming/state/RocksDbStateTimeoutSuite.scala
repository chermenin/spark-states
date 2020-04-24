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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.testing.FakeTicker
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfter, FunSuite}
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider.{DUMMY_VALUE, MapType}

import scala.util.Random

/**
  * @author Chitral Verma
  * @since 10/30/18
  */
class RocksDbStateTimeoutSuite extends FunSuite with BeforeAndAfter {

  import RocksDbStateStoreHelper._
  import StateStoreTestsHelper._

  final val testDBLocation: String = "testdb"

  private def withTTLStore(ttl: Long, sqlConf: SQLConf, pathSuffix: String = "")
                  (f: (FakeTicker, StateStore) => Unit): Unit = {
    val (ticker, stateStore) = createTTLStore(ttl, sqlConf, testDBLocation + pathSuffix)

    f(ticker, stateStore)
    stateStore.commit()
  }

  private def stopAndCleanUp(): Unit = {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
    performCleanUp(testDBLocation)
  }

  before {
    stopAndCleanUp()
  }

  after {
    stopAndCleanUp()
  }

  test("no timeout") {
    val expireTime = -1
    val sqlConf = createSQLConf(expireTime, isStrict = true)

    withTTLStore(expireTime, sqlConf)((ticker, store) => {
      put(store, "k1", 1)
      ticker.advance(20, TimeUnit.SECONDS)

      assert(size(store) === 1)
      assert(contains(store, "k1"))

      ticker.advance(Long.MaxValue, TimeUnit.SECONDS)

      assert(size(store) === 1)
      assert(contains(store, "k1"))
    })
  }

  test("statelessness") {
    val expireTime = 0
    val sqlConf = createSQLConf(expireTime, isStrict = true)

    withTTLStore(expireTime, sqlConf)((_, store) => {
      put(store, "k1", 1)

      assert(size(store) === 0)
      assert(!contains(store, "k1"))

      put(store, "k1", 1)
      put(store, "k2", 1)
      put(store, "k3", 1)

      assert(size(store) === 0)
      assert(!contains(store, "k1"))
      assert(!contains(store, "k2"))
      assert(!contains(store, "k3"))
    })
  }

  test("processing timeout") {
    val expireTime = 5
    val sqlConf = createSQLConf(expireTime, isStrict = true)

    withTTLStore(expireTime, sqlConf)((ticker, store) => {
      put(store, "k1", 1)

      ticker.advance(3, TimeUnit.SECONDS)

      assert(size(store) === 1)
      assert(contains(store, "k1"))

      ticker.advance(expireTime - 3, TimeUnit.SECONDS)

      assert(size(store) === 0)
      assert(!contains(store, "k1"))
    })
  }

  test("ttl should reset on get, set and update") {
    val expireTime = 5
    val sqlConf = createSQLConf(expireTime, isStrict = true)

    withTTLStore(expireTime, sqlConf)((ticker, store) => {
      put(store, "k1", 1)
      put(store, "k2", 1)
      ticker.advance(3, TimeUnit.SECONDS)

      assert(size(store) === 2)
      assert(contains(store, "k1"))

      put(store, "k1", 2) // reset timeout for k1
      ticker.advance(2, TimeUnit.SECONDS) // deadline met for k2

      assert(size(store) === 1)
      assert(!contains(store, "k2"))

      ticker.advance(2, TimeUnit.SECONDS)

      assert(size(store) === 1) // 1 second remains for k1 here
      assert(contains(store, "k1"))

      ticker.advance(1, TimeUnit.SECONDS) // deadline met for k1

      put(store, "k3", 3)

      assert(size(store) === 1)
      assert(!contains(store, "k1"))

      ticker.advance(4, TimeUnit.SECONDS) // 1 second remains for k3 here

      assert(size(store) === 1)
      assert(contains(store, "k3"))

      get(store, "k3") // reset timeout for k3

      ticker.advance(1, TimeUnit.SECONDS)

      assert(size(store) === 1)
      assert(contains(store, "k3"))

      ticker.advance(4, TimeUnit.SECONDS) // deadline met for k3

      assert(size(store) === 0)
      assert(!contains(store, "k3"))
    })
  }

  test("different timeouts for each streaming query (states)") {
    // Each query creates its own state store, the SQLConf is the same
    import RocksDbStateStoreProvider.STATE_EXPIRY_SECS
    val query1 = "query1"
    val timeout1 = 3

    val query2 = "query2"
    val timeout2 = 5

    val sqlConf = createSQLConf(isStrict = true, configs = Map(
      s"$STATE_EXPIRY_SECS.$query1" -> s"$timeout1",
      s"$STATE_EXPIRY_SECS.$query2" -> s"$timeout2"
    ))

    withTTLStore(timeout1, sqlConf, "1")((ticker1, store1) => {
      withTTLStore(timeout2, sqlConf, "2")((ticker2, store2) => {

        // Same data is read by both queries
        put(store1, "k1", 1)
        put(store1, "k2", 1)
        put(store2, "k1", 1)
        put(store2, "k2", 1)

        assert(size(store1) === 2)
        assert(contains(store1, "k1"))
        assert(contains(store1, "k2"))

        assert(size(store2) === 2)
        assert(contains(store2, "k1"))
        assert(contains(store2, "k2"))

        // Clock progression is the same for both queries
        ticker1.advance(2, TimeUnit.SECONDS)
        ticker2.advance(2, TimeUnit.SECONDS)

        assert(size(store1) === 2)
        assert(contains(store1, "k1"))
        assert(contains(store1, "k2"))

        assert(size(store2) === 2)
        assert(contains(store2, "k1"))
        assert(contains(store2, "k2"))

        ticker1.advance(1, TimeUnit.SECONDS) // deadline met for query1
        ticker2.advance(1, TimeUnit.SECONDS)

        assert(size(store1) === 0)
        assert(!contains(store1, "k1"))
        assert(!contains(store1, "k2"))

        assert(size(store2) === 2)
        assert(contains(store2, "k1"))
        assert(contains(store2, "k2"))

        ticker1.advance(2, TimeUnit.SECONDS)
        ticker2.advance(2, TimeUnit.SECONDS) // deadline met for query2

        assert(size(store1) === 0)
        assert(!contains(store1, "k1"))
        assert(!contains(store1, "k2"))

        assert(size(store2) === 0)
        assert(!contains(store2, "k1"))
        assert(!contains(store2, "k2"))


      })
    })
  }

  private def createTTLStore(ttl: Long, sqlConf: SQLConf, dbPath: String): (FakeTicker, StateStore) = {

    def createMockCache(ttl: Long, ticker: Ticker): MapType = {
      val loader = new CacheLoader[UnsafeRow, String] {
        override def load(key: UnsafeRow): String = DUMMY_VALUE
      }

      val cacheBuilder = CacheBuilder.newBuilder()

      val cacheBuilderWithOptions = {
        if (ttl >= 0) {
          cacheBuilder
            .expireAfterAccess(ttl, TimeUnit.SECONDS)
            .ticker(ticker)
        } else
          cacheBuilder
      }

      cacheBuilderWithOptions.build[UnsafeRow, String](loader)
    }

    val ticker = new FakeTicker
    val cache = createMockCache(ttl, ticker)

    val provider = createStoreProvider(opId = Random.nextInt(), partition = Random.nextInt(), sqlConf = sqlConf)
    val store = new provider.RocksDbStateStore(0, dbPath, keySchema, valueSchema, new ConcurrentHashMap, cache)

    (ticker, store)
  }

}

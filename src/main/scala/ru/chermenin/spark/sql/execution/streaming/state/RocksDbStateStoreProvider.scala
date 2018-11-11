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

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path => LocalPath, _}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
  * by files in a HDFS-compatible file system using RocksDB key-value storage format.
  * All updates to the store has to be done in sets transactionally, and each set of updates
  * increments the store's version. These versions can be used to re-execute the updates
  * (by retries in RDD operations) on the correct version of the store, and regenerate
  * the store version.
  *
  * Usage:
  * To update the data in the state store, the following order of operations are needed.
  *
  * // get the right store
  * - val store = StateStore.get(
  * StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
  * - store.put(...)
  * - store.remove(...)
  * - store.commit()    // commits all the updates to made; the new version will be returned
  * - store.iterator()  // key-value data after last commit as an iterator
  * - store.updates()   // updates made in the last commit as an iterator
  *
  * Fault-tolerance model:
  * - Every set of updates is written to a snapshot file before committing.
  * - The state store is responsible for cleaning up of old snapshot files.
  * - Multiple attempts to commit the same version of updates may overwrite each other.
  * Consistency guarantees depend on whether multiple attempts have the same updates and
  * the overwrite semantics of underlying file system.
  * - Background maintenance of files ensures that last versions of the store is always
  * recoverable to ensure re-executed RDD operations re-apply updates on the correct
  * past version of the store.
  *
  * Description of State Timeout API
  * --------------------------------
  *
  * This API should be used to open the db when key-values inserted are
  * meant to be removed from the db in 'ttl' amount of time provided in seconds.
  * The timeouts can be optionally set to strict expiration by setting
  * spark.sql.streaming.stateStore.strictExpire = true on `SparkConf`
  *
  * Timeout Modes:
  *  - In non strict mode (default), this guarantees that key-values inserted will remain in the db
  *    for >= ttl amount of time and the db will make efforts to remove the key-values
  *    as soon as possible after ttl seconds of their insertion.
  *  - In strict mode, the key-values inserted will remain in the db for exactly ttl amount of time.
  *    To ensure exact expiration, a separate cache of keys is maintained in memory with
  *    their respective deadlines and is used for reference during operations.
  *
  * This API can be used to allow,
  *  - Stateless Processing - set timeout to 0
  *  - Infinite State (no timeout) - set timeout to -1, which is set by default.
  */
class RocksDbStateStoreProvider extends StateStoreProvider with Logging {


  /** Load native RocksDb library */
  RocksDB.loadLibrary()

  private val options: Options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(RocksDbStateStoreProvider.DEFAULT_WRITE_BUFFER_SIZE_MB * SizeUnit.MB)
    .setMaxWriteBufferNumber(RocksDbStateStoreProvider.DEFAULT_WRITE_BUFFER_NUMBER)
    .setMaxBackgroundCompactions(RocksDbStateStoreProvider.DEFAULT_BACKGROUND_COMPACTIONS)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  /** Implementation of [[StateStore]] API which is backed by RocksDB */
  class RocksDbStateStore(val version: Long,
                          val dbPath: String,
                          val keySchema: StructType,
                          val valueSchema: StructType,
                          val localSnapshots: ConcurrentHashMap[Long, String],
                          val keyCache: MapType) extends StateStore {

    /** New state version */
    private val newVersion = version + 1

    /** RocksDb database to keep state */
    private val store: RocksDB = TtlDB.open(options, dbPath, ttlSec, false)

    /** Enumeration representing the internal state of the store */
    object State extends Enumeration {
      val Updating, Committed, Aborted = Value
    }

    @volatile private var keysNumber: Long = 0
    @volatile private var state: State.Value = State.Updating

    /** Unique identifier of the store */
    override def id: StateStoreId = RocksDbStateStoreProvider.this.stateStoreId

    /**
      * Get the current value of a non-null key.
      *
      * @return a non-null row if the key exists in the store, otherwise null.
      */
    override def get(key: UnsafeRow): UnsafeRow = {
      if (isStrictExpire) {
        Option(keyCache.getIfPresent(key)) match {
          case Some(_) => getValue(key)
          case None => null
        }
      } else getValue(key)
    }

    /**
      * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
      * the params can be reused, and must make copies of the data as needed for persistence.
      */
    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == State.Updating, "Cannot put entry into already committed or aborted state")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      synchronized {
        store.put(keyCopy.getBytes, valueCopy.getBytes)

        if (isStrictExpire)
          keyCache.put(keyCopy, DUMMY_VALUE)
      }
    }

    /**
      * Remove a single non-null key.
      */
    override def remove(key: UnsafeRow): Unit = {
      verify(state == State.Updating, "Cannot remove entry from already committed or aborted state")
      synchronized {
        store.delete(key.getBytes)

        if (isStrictExpire)
          keyCache.invalidate(key.getBytes)
      }
    }

    /**
      * Get key value pairs with optional approximate `start` and `end` extents.
      * If the State Store implementation maintains indices for the data based on the optional
      * `keyIndexOrdinal` over fields `keySchema` (see `StateStoreProvider.init()`), then it can use
      * `start` and `end` to make a best-effort scan over the data. Default implementation returns
      * the full data scan iterator, which is correct but inefficient. Custom implementations must
      * ensure that updates (puts, removes) can be made while iterating over this iterator.
      *
      * @param start UnsafeRow having the `keyIndexOrdinal` column set with appropriate starting value.
      * @param end   UnsafeRow having the `keyIndexOrdinal` column set with appropriate ending value.
      * @return An iterator of key-value pairs that is guaranteed not miss any key between start and
      *         end, both inclusive.
      */
    override def getRange(start: Option[UnsafeRow], end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
      verify(state == State.Updating, "Cannot getRange from already committed or aborted state")
      iterator()
    }

    /**
      * Commit all the updates that have been made to the store, and return the new version.
      */
    override def commit(): Long = {
      verify(state == State.Updating, "Cannot commit already committed or aborted state")

      try {
        state = State.Committed
        keysNumber =
          if (isStrictExpire) keyCache.size
          else store.getLongProperty(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY)

        store.close()
        putLocalSnapshot(newVersion, dbPath)
        snapshot(newVersion, dbPath)
        logInfo(s"Committed version $newVersion for $this")
        newVersion
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(s"Error committing version $newVersion into $this", e)
      }
    }

    /**
      * Abort all the updates made on this store. This store will not be usable any more.
      */
    override def abort(): Unit = {
      verify(state != State.Committed, "Cannot abort already committed state")
      try {
        state = State.Aborted
        keysNumber =
          if (isStrictExpire) keyCache.size
          else store.getLongProperty(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY)

        store.close()
        putLocalSnapshot(newVersion + 1, dbPath)
        logInfo(s"Aborted version $newVersion for $this")
      } catch {
        case e: Exception =>
          logWarning(s"Error aborting version $newVersion into $this", e)
      }
    }

    /**
      * Get an iterator of all the store data.
      * This can be called only after committing all the updates made in the current thread.
      */
    override def iterator(): Iterator[UnsafeRowPair] = {
      val stateFromRocksIter: Iterator[UnsafeRowPair] = new Iterator[UnsafeRowPair] {

        /** Internal RocksDb iterator */
        private val iterator = store.newIterator()
        iterator.seekToFirst()

        /** Check if has some data */
        override def hasNext: Boolean = iterator.isValid

        /** Get next data from RocksDb */
        override def next(): UnsafeRowPair = {
          iterator.status()

          val key = new UnsafeRow(keySchema.fields.length)
          val keyBytes = iterator.key()
          key.pointTo(keyBytes, keyBytes.length)

          val value = new UnsafeRow(valueSchema.fields.length)
          val valueBytes = iterator.value()
          value.pointTo(valueBytes, valueBytes.length)

          iterator.next()

          new UnsafeRowPair(key, value)
        }
      }

      val stateIter = {
        val keys = keyCache.asMap().keySet()

        if (isStrictExpire) stateFromRocksIter.filter(x => keys.contains(x.key))
        else stateFromRocksIter
      }

      stateIter
    }

    /**
      * Returns current metrics of the state store
      */
    override def metrics: StateStoreMetrics =
      StateStoreMetrics(keysNumber, keysNumber * (keySchema.defaultSize + valueSchema.defaultSize), Map.empty)

    /**
      * Whether all updates have been committed
      */
    override def hasCommitted: Boolean = state == State.Committed

    /**
      * Custom toString implementation for this state store class.
      */
    override def toString: String =
      s"RocksDbStateStore[id=(op=${id.operatorId},part=${id.partitionId}),localDir=$dbPath,snapshotsDir=$baseDir]"

    /**
      * Method to put current DB path to local snapshots list.
      */
    private def putLocalSnapshot(version: Long, dbPath: String): Unit = {
      localSnapshots.keys().asScala
        .filter(_ < version - storeConf.minVersionsToRetain)
        .foreach(version => deleteFile(localSnapshots.get(version)))
      localSnapshots.put(version, dbPath)
    }

    private def getValue(key: UnsafeRow): UnsafeRow = {
      val valueBytes = store.get(key.getBytes)
      if (valueBytes == null) return null
      val value = new UnsafeRow(valueSchema.fields.length)
      value.pointTo(valueBytes, valueBytes.length)
      value
    }
  }

  /* Internal fields and methods */
  private val localSnapshots: ConcurrentHashMap[Long, String] = new ConcurrentHashMap[Long, String]()

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var tempDir: String = _
  @volatile private var ttlSec: Int = _
  @volatile private var isStrictExpire: Boolean = _

  private def baseDir: Path = stateStoreId.storeCheckpointLocation()

  private def fs: FileSystem = baseDir.getFileSystem(hadoopConf)

  /**
    * Initialize the provide with more contextual information from the SQL operator.
    * This method will be called first after creating an instance of the StateStoreProvider by
    * reflection.
    *
    * @param stateStoreId Id of the versioned StateStores that this provider will generate
    * @param keySchema    Schema of keys to be stored
    * @param valueSchema  Schema of value to be stored
    * @param indexOrdinal Optional column (represent as the ordinal of the field in keySchema) by
    *                     which the StateStore implementation could index the data.
    * @param storeConf    Configurations used by the StateStores
    * @param hadoopConf   Hadoop configuration that could be used by StateStore to save state data
    */
  override def init(stateStoreId: StateStoreId,
                    keySchema: StructType,
                    valueSchema: StructType,
                    indexOrdinal: Option[Int],
                    storeConf: StateStoreConf,
                    hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.tempDir = getTempDir(getTempPrefix(hadoopConf.get("spark.app.name")), "")
    this.ttlSec = setTTL(storeConf.confs)
    this.isStrictExpire = setExpireMode(storeConf.confs)

    fs.mkdirs(baseDir)
  }

  /**
    * Get the state store for making updates to create a new `version` of the store.
    */
  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")

    val snapshotVersions = fetchVersions()
    val localVersions = localSnapshots.keySet().asScala
    val versions = (snapshotVersions ++ localVersions).filter(_ <= version)

    def initStateStore(path: String): StateStore =
      new RocksDbStateStore(version, path, keySchema, valueSchema, localSnapshots, createCache(ttlSec))

    val stateStore = versions.sorted(Ordering.Long.reverse)
      .map(version => Try(loadDb(version)).map(initStateStore))
      .find(_.isSuccess).map(_.get)
      .getOrElse(initStateStore(getTempDir(getTempPrefix(), s".$version")))

    logInfo(s"Retrieved $stateStore for version $version of ${RocksDbStateStoreProvider.this} for update")
    stateStore
  }

  /**
    * Return the id of the StateStores this provider will generate.
    */
  override def stateStoreId: StateStoreId = stateStoreId_

  /**
    * Do maintenance backing data files, including cleaning up old files
    */
  override def doMaintenance(): Unit = {
    try {
      cleanup()
    } catch {
      case NonFatal(ex) =>
        logWarning(s"Error cleaning up $this", ex)
    }
  }

  /**
    * Called when the provider instance is unloaded from the executor.
    */
  override def close(): Unit = {
    deleteFile(tempDir)
  }

  /**
    * Custom toString implementation for this state store provider class.
    */
  override def toString: String = {
    s"RocksDbStateStoreProvider[" +
      s"id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),dir = $baseDir]"
  }

  /**
    * Remove CRC files and old logs before uploading.
    */
  private def removeCrcAndLogs(dbPath: String): Boolean = {
    new File(dbPath).listFiles().filter(file => {
      val name = file.getName.toLowerCase()
      name.endsWith(".crc") || name.startsWith("log.old")
    }).forall(_.delete())
  }

  /**
    * Finalize snapshot by moving local RocksDb files into HDFS as zipped file.
    */
  private def snapshot(version: Long, dbPath: String): Path = synchronized {
    val snapshotFile = getSnapshotFile(version)
    try {
      if (removeCrcAndLogs(dbPath)) {
        compress(new File(dbPath).listFiles(), snapshotFile)
        logInfo(s"Saved snapshot for version $version in $snapshotFile")
      } else {
        throw new IOException(s"Failed to delete CRC files or old logs before moving $dbPath to $snapshotFile")
      }
    } catch {
      case ex: Exception =>
        throw new IOException(s"Failed to move $dbPath to $snapshotFile", ex)
    }
    snapshotFile
  }

  /**
    * Load the required version of the RocksDb files from HDFS.
    */
  private def loadDb(version: Long): String = {
    val dbPath = getTempDir(getTempPrefix(), s".$version")
    if (hasLocalSnapshot(version) && loadLocalSnapshot(version, dbPath) || loadHdfsSnapshot(version, dbPath)) {
      dbPath
    } else {
      throw new IOException(s"Failed to load state snapshot for version $version")
    }
  }

  /**
    * Returns true if there is a local snapshot directory for the version.
    */
  private def hasLocalSnapshot(version: Long): Boolean =
    localSnapshots.containsKey(version)

  /**
    * Move files from local snapshot to reuse in new version.
    */
  private def loadLocalSnapshot(version: Long, dbPath: String): Boolean = {
    val localSnapshotDir = localSnapshots.remove(version)
    try {
      Files.move(
        Paths.get(localSnapshotDir), Paths.get(dbPath),
        StandardCopyOption.REPLACE_EXISTING
      )
      true
    } catch {
      case ex: Exception =>
        logWarning(s"Failed to reuse $localSnapshotDir as $dbPath", ex)
        false
    }
  }

  /**
    * Load files from distributed file system to use in new version of state.
    */
  private def loadHdfsSnapshot(version: Long, dbPath: String): Boolean = {
    val snapshotFile = getSnapshotFile(version)
    try {
      decompress(snapshotFile, dbPath)
      true
    } catch {
      case ex: Exception =>
        throw new IOException(s"Failed to load from $snapshotFile to $dbPath", ex)
    }
  }

  /**
    * Save RocksDB files as ZIP archive in HDFS as snapshot.
    */
  private def compress(files: Array[File], snapshotFile: Path): Unit = {
    val buffer = new Array[Byte](hadoopConf.getInt("io.file.buffer.size", 4096))
    val output = new ZipOutputStream(fs.create(snapshotFile))
    try {
      files.foreach(file => {
        val input = new FileInputStream(file)
        try {
          output.putNextEntry(new ZipEntry(file.getName))
          Iterator.continually(input.read(buffer))
            .takeWhile(_ != -1)
            .filter(_ > 0)
            .foreach(read =>
              output.write(buffer, 0, read)
            )
          output.closeEntry()
        } finally {
          input.close()
        }
      })
    } finally {
      output.close()
    }
  }

  /**
    * Load archive from HDFS and unzip RocksDB files.
    */
  private def decompress(snapshotFile: Path, dbPath: String): Unit = {
    val buffer = new Array[Byte](hadoopConf.getInt("io.file.buffer.size", 4096))
    val input = new ZipInputStream(fs.open(snapshotFile))
    try {
      Iterator.continually(input.getNextEntry)
        .takeWhile(_ != null)
        .foreach(entry => {
          val output = new FileOutputStream(s"$dbPath${File.separator}${entry.getName}")
          try {
            Iterator.continually(input.read(buffer))
              .takeWhile(_ != -1)
              .filter(_ > 0)
              .foreach(read =>
                output.write(buffer, 0, read)
              )
          } finally {
            output.close()
          }
        })
    } finally {
      input.close()
    }
  }

  /**
    * Clean up old snapshots that are not needed any more. It ensures that last
    * few versions of the store can be recovered from the files, so re-executed RDD operations
    * can re-apply updates on the past versions of the store.
    */
  private def cleanup(): Unit = {
    try {
      val versions = fetchVersions()
      if (versions.nonEmpty) {
        val earliestVersionToRetain = versions.max - storeConf.minVersionsToRetain + 1

        val filesToDelete = versions
          .filter(_ < earliestVersionToRetain)
          .map(getSnapshotFile)

        if (filesToDelete.nonEmpty) {
          filesToDelete.foreach(fs.delete(_, true))
          logInfo(s"Deleted files older than $earliestVersionToRetain for $this: ${filesToDelete.mkString(", ")}")
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error cleaning up files for $this", e)
    }
  }

  /**
    * Fetch all versions that back the store.
    */
  private def fetchVersions(): Seq[Long] = {
    val files: Seq[FileStatus] = try {
      fs.listStatus(baseDir)
    } catch {
      case _: java.io.FileNotFoundException =>
        Seq.empty
    }
    files.flatMap { status =>
      val path = status.getPath
      val nameParts = path.getName.split("\\.")
      if (nameParts.size == 3) {
        Seq(nameParts(2).toLong)
      } else {
        Seq()
      }
    }
  }

  /**
    * Get path to snapshot file for the version.
    */
  private def getSnapshotFile(version: Long): Path =
    new Path(baseDir, s"state.snapshot.$version")

  /**
    * Get full prefix for local temp directory.
    *
    * @return
    */
  private def getTempPrefix(prefix: String = "state"): String =
    s"$prefix-${stateStoreId_.operatorId}-${stateStoreId_.partitionId}-${stateStoreId_.storeName}-"

  /**
    * Create local temporary directory.
    */
  private def getTempDir(prefix: String, suffix: String): String = {
    val file = if (tempDir != null) {
      File.createTempFile(prefix, suffix, new File(tempDir)).getAbsoluteFile
    } else {
      File.createTempFile(prefix, suffix).getAbsoluteFile
    }
    if (file.delete() && file.mkdirs()) {
      file.getAbsolutePath
    } else {
      throw new IOException(s"Failed to create temp directory ${file.getAbsolutePath}")
    }
  }

  /**
    * Verify the condition and rise an exception if the condition is failed.
    */
  private def verify(condition: => Boolean, msg: String): Unit =
    if (!condition) throw new IllegalStateException(msg)

  /**
    * Get iterator of all the data of the latest version of the store.
    * Note that this will look up the files to determined the latest known version.
    */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = {
    val versions = fetchVersions()
    if (versions.nonEmpty) {
      getStore(versions.max).iterator()
    } else Iterator.empty
  }

  /**
    * Method to delete directory or file.
    */
  private def deleteFile(path: String): Unit = {
    Files.walkFileTree(Paths.get(path), new SimpleFileVisitor[LocalPath] {

      override def visitFile(visitedFile: LocalPath, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(visitedFile)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(visitedDirectory: LocalPath, exc: IOException): FileVisitResult = {
        Files.delete(visitedDirectory)
        FileVisitResult.CONTINUE
      }
    })
  }


}

/**
  * Companion object with constants.
  */
object RocksDbStateStoreProvider {
  type MapType = com.google.common.cache.LoadingCache[UnsafeRow, String]

  /** Default write buffer size for RocksDb in megabytes */
  val DEFAULT_WRITE_BUFFER_SIZE_MB = 200

  /** Default number of write buffers for RocksDb */
  val DEFAULT_WRITE_BUFFER_NUMBER = 3

  /** Default background compactions value for RocksDb */
  val DEFAULT_BACKGROUND_COMPACTIONS = 10

  val ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY = "rocksdb.estimate-num-keys"

  final val STATE_EXPIRY_SECS: String = "spark.sql.streaming.stateStore.stateExpirySecs"

  final val DEFAULT_STATE_EXPIRY_SECS: String = "-1"

  final val STATE_EXPIRY_STRICT_MODE: String = "spark.sql.streaming.stateStore.strictExpire"

  final val DEFAULT_STATE_EXPIRY_METHOD: String = "false"

  final val DUMMY_VALUE: String = ""

  private def createCache(stateTtlSecs: Long): MapType = {
    val loader = new CacheLoader[UnsafeRow, String] {
      override def load(key: UnsafeRow): String = DUMMY_VALUE
    }

    val cacheBuilder = CacheBuilder.newBuilder()

    val cacheBuilderWithOptions = {
      if (stateTtlSecs >= 0)
        cacheBuilder.expireAfterAccess(stateTtlSecs, TimeUnit.SECONDS)
      else
        cacheBuilder
    }

    cacheBuilderWithOptions.build[UnsafeRow, String](loader)
  }

  private def setTTL(conf: Map[String, String]): Int =

    Try(conf.getOrElse(STATE_EXPIRY_SECS, DEFAULT_STATE_EXPIRY_SECS).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setExpireMode(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_EXPIRY_STRICT_MODE, DEFAULT_STATE_EXPIRY_METHOD).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

}

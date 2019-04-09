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

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.util.concurrent.TimeUnit
import java.util.zip.ZipInputStream

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, PathFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType
import org.rocksdb.{TickerType, _}
import org.rocksdb.util.SizeUnit
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

/**
  * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
  * by files in a HDFS-compatible file system using RocksDB key-value storage format.
  * All updates to the store has to be done in sets transactionally, and each set of updates
  * increments the store's version. These versions can be used to re-execute the updates
  * (by retries in RDD operations) on the correct version of the store, and regenerate
  * the store version.
  * RocksDB Files will be kept in local filesystem and state store versions are uploaded to remote
  * filesystem on commit. To do efficient versioning, RocksDB's backup functionality can be used which
  * saves incremental backups to another location in the local filesystem.
  *
  * Usage:
  * To update the data in the state store, the following order of operations are needed:
  * // get the right store
  * - val store = StateStore.get(StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
  * - store.put(...)
  * - store.remove(...)
  * - store.commit()    // commits all the updates to made; the new version will be returned
  * - store.iterator()  // key-value data after last commit as an iterator
  *
  * Fault-tolerance model:
  * - Every set of updates is written to RocksDB WAL (write ahead log) while committing.
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
  *
  * Notes
  * -----
  * - Attention: On windows deletion of Files which are written over java.nio is not possible in current java process.
  * This is an old bug which might be solved in Java 10 or 11, see https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
  *
  *  TODO: What happens when backupId overflows (backupId is an integer, but version is long)
  *  TODO: Enable checksum in BackupOptions again?
  */
class RocksDbStateStoreProvider extends StateStoreProvider with Logging {

  /** Load native RocksDb library */
  RocksDB.loadLibrary()

  private var options: Options = _
  private var writeOptions: WriteOptions = _

  /** Implementation of [[StateStore]] API which is backed by RocksDB */
  class RocksDbStateStore(val version: Long,
                          val keySchema: StructType,
                          val valueSchema: StructType,
                          val keyCache: MapType) extends StateStore {

    /** New state version */
    private val newVersion = version + 1

    /** Enumeration representing the internal state of the store */
    object State extends Enumeration {
      val Updating, Committed, Aborted = Value
    }

    @volatile private var rocksdbVolumeStatistics: Map[String,Long] = Map()
    @volatile private var state: State.Value = State.Updating

    /** Unique identifier of the store */
    override def id: StateStoreId = RocksDbStateStoreProvider.this.stateStoreId

    /**
      * Get the current value of a non-null key.
      *
      * @return a non-null row if the key exists in the store, otherwise null.
      */
    override def get(key: UnsafeRow): UnsafeRow = try {
      var returnValue: UnsafeRow = null
      val t = MiscHelper.measureTime{
        returnValue = if (isStrictExpire) {
          Option(keyCache.getIfPresent(key)) match {
            case Some(_) => getValue(key)
            case None => null
          }
        } else getValue(key)
      }
      val returnValueLog = if(returnValue!=null) "result size is "+returnValue.getSizeInBytes else "no value found"
      logInfo(s"get ${key.toSeq(keySchema)} took $t secs, $returnValueLog")
      returnValue
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'get' key ${key.toSeq(keySchema)} of $this")
        throw e
    }

    /**
      * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
      * the params can be reused, and must make copies of the data as needed for persistence.
      */
    override def put(key: UnsafeRow, value: UnsafeRow): Unit = try {
      MiscHelper.verify(state == State.Updating, "Cannot put entry into already committed or aborted state")
      val t = MiscHelper.measureTime {
        val keyCopy = key.copy()
        val valueCopy = value.copy()
        RocksDbStateStoreProvider.this.synchronized {
          currentDb.put(writeOptions, keyCopy.getBytes, valueCopy.getBytes)

          if (isStrictExpire)
            keyCache.put(keyCopy, DUMMY_VALUE)
        }
      }
      logInfo(s"put ${key.toSeq(keySchema)} took $t secs, size is ${value.getSizeInBytes}")
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'put' key ${key.toSeq(keySchema)} of $this")
        throw e
    }

    /**
      * Remove a single non-null key.
      */
    override def remove(key: UnsafeRow): Unit = try {
      MiscHelper.verify(state == State.Updating, "Cannot remove entry from already committed or aborted state")
      RocksDbStateStoreProvider.this.synchronized {
        currentDb.delete(key.getBytes)

        if (isStrictExpire)
          keyCache.invalidate(key.getBytes)
      }
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'remove' of $this")
        throw e
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
      MiscHelper.verify(state == State.Updating, "Cannot getRange from already committed or aborted state")
      iterator()
    }

    /**
      * Commit all the updates that have been made to the store, and return the new version.
      */
    override def commit(): Long = try {
      MiscHelper.verify(state == State.Updating, "Cannot commit already committed or aborted state")

      updateStatistics()

      state = State.Committed
      RocksDbStateStoreProvider.this.synchronized {
        createBackup(newVersion)
        if (closeDbOnCommit) {
          currentDb.close()
          currentDb = null
        }
      }

      logInfo(s"Committed version $newVersion for $this")
      newVersion

    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'commit' for version $newVersion of $this")
        throw e
    }

    def updateStatistics(): Unit = {
      // log statistics
      val statsTypes = Seq(
          TickerType.NUMBER_KEYS_READ, TickerType.NUMBER_KEYS_UPDATED, TickerType.NUMBER_KEYS_WRITTEN
        , TickerType.MEMTABLE_HIT, TickerType.MEMTABLE_MISS
        , TickerType.BLOCK_CACHE_HIT, TickerType.BLOCK_CACHE_MISS
        , TickerType.BYTES_READ, TickerType.BYTES_WRITTEN
      )
      logInfo(s"rocksdb cache statistics for $this: "+statsTypes.map( t => s"$t=${currentDbStats.getAndResetTickerCount(t)}" ).mkString(" "))

      //log volume statistics
      rocksdbVolumeStatistics = ROCKSDB_VOLUME_PROPERTIES.map{ p =>
        if (p==ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY && isStrictExpire) p->keyCache.size
        else p->currentDb.getLongProperty(p)
      }.toMap
      logInfo(s"rocksdb volume statistics for $this: "+rocksdbVolumeStatistics.map{ case (k,v) => s"$k=$v"}.mkString(" "))
    }

    /**
      * Abort all the updates made on this store. This store will not be usable any more.
      */
    override def abort(): Unit = try {
      MiscHelper.verify(state != State.Committed, "Cannot abort already committed state")
      //TODO: how can we rollback uncommitted changes -> we should use a transaction!

      updateStatistics()

      state = State.Aborted
      if (closeDbOnCommit) RocksDbStateStoreProvider.this.synchronized {
        currentDb.close()
        currentDb = null
      }

      logInfo(s"Aborted version $newVersion for $this")

    } catch {
      case e: Exception =>
        logWarning(s"Error aborting version $newVersion into $this", e)
    }

    /**
      * Get an iterator of all the store data.
      * This can be called only after committing all the updates made in the current thread.
      */
    override def iterator(): Iterator[UnsafeRowPair] = {
      val stateFromRocksIter: Iterator[UnsafeRowPair] = new Iterator[UnsafeRowPair] {

        /** Internal RocksDb iterator */
        private val iterator = currentDb.newIterator()
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
        keyCache.cleanUp()
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
      StateStoreMetrics( rocksdbVolumeStatistics.getOrElse(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY,0)
                       , rocksdbVolumeStatistics.getOrElse(ROCKSDB_SIZE_ALL_MEM_TABLES,0), Map.empty)

    /**
      * Whether all updates have been committed
      */
    override def hasCommitted: Boolean = state == State.Committed

    /**
      * Custom toString implementation for this state store class.
      */
    override def toString: String =
      s"RocksDbStateStore[op=${id.operatorId},part=${id.partitionId},remoteDir=$remoteBackupPath]"

    def getCommittedVersion: Option[Long] = if(hasCommitted) Some(newVersion) else None

    private def getValue(key: UnsafeRow): UnsafeRow = {
      val valueBytes = currentDb.get(key.getBytes)
      if (valueBytes == null) return null
      val value = new UnsafeRow(valueSchema.fields.length)
      value.pointTo(valueBytes, valueBytes.length)
      value
    }
  }

  /* Internal fields and methods */
  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var localDataDir: String = _
  @volatile private var localWalDataDir: String = _
  @volatile private var ttlSec: Int = _
  @volatile private var isStrictExpire: Boolean = _
  @volatile private var rotatingBackupKey: Boolean = _
  @volatile private var closeDbOnCommit: Boolean = _
  private var remoteBackupPath: Path = _
  private var localBackupPath: Path = _
  private var localBackupFs: FileSystem = _
  private var localBackupDir: String = _
  private var localDbDir: String = _
  private var currentDb: RocksDB = _
  private val currentDbStats: Statistics = new Statistics()
  private var currentStore: RocksDbStateStore = _
  private var backupEngine: BackupEngine = _
  private var backupList: mutable.Map[Long,(Int,String)] = mutable.HashMap()
  private var remoteCleanupList: mutable.ArrayBuffer[Path] = mutable.ArrayBuffer()
  private var remoteBackupFm: CheckpointFileManager = _

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
                    hadoopConf: Configuration): Unit = try {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.localDataDir = MiscHelper.createLocalDir( setLocalDir(storeConf.confs)+"/"+getDataDirName(hadoopConf.get("spark.app.name")))
    this.localWalDataDir = MiscHelper.createLocalDir( setLocalWalDir(storeConf.confs)+"/"+getDataDirName(hadoopConf.get("spark.app.name")))
    this.ttlSec = setTTL(storeConf.confs)
    this.isStrictExpire = setExpireMode(storeConf.confs)
    this.rotatingBackupKey = setRotatingBackupKey(storeConf.confs)
    this.closeDbOnCommit = setCloseDbAfterCommit(storeConf.confs)

    // initialize paths
    remoteBackupPath = stateStoreId.storeCheckpointLocation()
    localBackupDir = MiscHelper.createLocalDir(localDataDir+"/backup")
    localBackupPath = new Path("file:///"+localBackupDir)
    localBackupFs = localBackupPath.getFileSystem(hadoopConf)
    localDbDir = MiscHelper.createLocalDir(localDataDir+"/db")
    remoteBackupFm = CheckpointFileManager.create(remoteBackupPath, hadoopConf)

    // initialize empty database
    currentDbStats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS)

    options = new Options()
      .setStatistics(currentDbStats)
      .setCreateIfMissing(true)
      .setWriteBufferSize(setWriteBufferSizeMb(storeConf.confs) * SizeUnit.MB)
      .setMaxWriteBufferNumber( setWriteBufferNumber(storeConf.confs))
      .setMaxWriteBufferNumber(RocksDbStateStoreProvider.DEFAULT_WRITE_BUFFER_NUMBER)
      // .setDbWriteBufferSize() // this is the same as setWriteBufferSize*setMaxWriteBufferNumber
      .setAllowMmapReads(false) // could be responsible for memory leaks according to https://github.com/facebook/rocksdb/issues/3216
      .setAllowMmapWrites(false) // this could make memory leaks according to https://github.com/facebook/rocksdb/issues/3216
      //.setTargetFileSizeBase(xxx * SizeUnit.MB))
      .setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(false).setBlockCacheSize(setBlockCacheSizeMb(storeConf.confs) * SizeUnit.MB))
      .setDisableAutoCompactions(true) // we trigger manual compaction during state maintenance with compactRange()
      .setWalDir(localWalDataDir) // Write-ahead-log should be saved on fast disk (local, not NAS...)
      .setCompressionType(setCompressionType(storeConf.confs))
      .setCompactionStyle(CompactionStyle.UNIVERSAL)
    writeOptions = new WriteOptions()
      .setDisableWAL(false) // we use write ahead log for efficient incremental state versioning

    restoreFromRemoteBackups()

    logInfo(s"initialized $this")
  } catch {
    case e:Exception =>
      logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'init' of $this")
      throw e
  }

  def restoreFromRemoteBackups(): Unit = {
    // copy backups from remote storage and init backup engine
    val backupDBOptions = new BackupableDBOptions(localBackupDir.toString)
      .setShareTableFiles(true)
      .setShareFilesWithChecksum(false)
      .setSync(true)
    backupList.clear
    if (remoteBackupFm.exists(remoteBackupPath)) {
      logDebug(s"loading state backup from remote filesystem $remoteBackupPath for $this")

      // read index file into backupList, otherwise extract backup information from archive files
      Try(remoteBackupFm.open(new Path(remoteBackupPath, "index"))) match {
        case Success(backupListInputStream) =>
          val backupListReader = new BufferedReader(new InputStreamReader(backupListInputStream))
          val backupListParsed = backupListReader.lines.iterator.asScala.toArray.map(_.split(',').map(_.trim.split(':')).map(e => (e(0).trim, e(1).trim)).toMap)
          backupListInputStream.close() // it is tricky that input stream isn't closed at all or even twice... iterator.toArray / manual close seems to be proper
          backupList ++= backupListParsed.map(b => b("version").toLong -> (b("backupId").toInt, b("backupKey"))).toMap
          logDebug(s"index contains the following backups for $this:" + backupList.toSeq.sortBy(_._1).map( b => s"v=${b._1},b=${b._2._1},k=${b._2._2}").mkString("; "))
        case Failure(_:FileNotFoundException) =>
          backupList ++= getBackupListFromRemoteFiles
          logWarning(s"index file not found on remote filesystem for $this. Extracted the following backup list of archive files: ${backupList.toSeq.sortBy(_._1).mkString(", ")}")
        case Failure(e) => throw e
      }

      // load files
      val t = MiscHelper.measureTime {

        // copy shared files in parallel
        remoteBackupFm.list(new Path(remoteBackupPath, "shared")).toSeq
          .par.foreach(f => copyRemoteToLocalFile(f.getPath, new Path(localBackupPath, s"shared/${f.getPath.getName}"), false))

        // copy metadata/private files according to backupList in parallel
        backupList.values.map{ case (backupId,backupKey) => s"$backupKey.zip" }.par
          .foreach(filename => try {
            MiscHelper.decompressFromRemote( new Path(remoteBackupPath, filename), localBackupDir, remoteBackupFm, getHadoopFileBufferSize)
          } catch {
            case e: FileNotFoundException => logWarning(s"{e.getMessage} when copying metadata/private files from remote backup in method 'init'")
          })
        backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)
        cleanupOldBackups()
      }
      logInfo(s"got state backup from remote filesystem $remoteBackupPath for $this, took $t secs")

    } else {
      logDebug(s"initializing state backup at $remoteBackupPath for $this")
      remoteBackupFm.mkdirs( new Path(remoteBackupPath, "shared"))
      backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)
      synchronized {
        currentDb = openDb
        createBackup(0L)
        currentDb.close()
        currentDb = null
      }
    }
  }

  /**
    * Get the state store for making updates to create a new `version` of the store.
    */
  override def getStore(version: Long): StateStore = try {
    getStore(version, createCache(ttlSec))
  } catch {
    case e:Exception =>
      logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'getStore' of $this for version $version")
      throw e
  }
  def getStore(version: Long, cache: MapType): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")

    // try restoring remote backups if version doesnt exists in local backups. This might happen if execution of a partition changes executor.
    if (!backupList.contains(version)) {
      restoreFromRemoteBackups()
      if (!backupList.contains(version)) throw new IllegalStateException(s"Can not find version $version in backup list (${backupList.keys.toSeq.sorted.mkString(",")})")
    }
    val t = MiscHelper.measureTime {
      restoreAndOpenDb(version)
      currentStore = new RocksDbStateStore(version, keySchema, valueSchema, cache)
    }
    logInfo(s"Retrieved $currentStore for $this version $version, took $t seconds")
    // return
    currentStore
  }

  protected def restoreAndOpenDb(version: Long): Unit = {
    logDebug(s"starting to restore db for $this version $version from local backup")

    // check if current db is desired version, else search in backups and restore
    if (currentStore!=null && currentStore.getCommittedVersion.contains(version)) {
      if (currentDb==null) synchronized {
        currentDb = openDb
      }
      logDebug(s"Current db already has correct version $version. No restore required for $this.")
    } else {

      // get infos about backup to recover
      val backupIdRecovery = backupList.get(version).map{ case (b,k) => b }
        .getOrElse(throw new IllegalStateException(s"backup for version $version not found"))

      // close db and restore backup
      val tRestore = MiscHelper.measureTime {
        synchronized {
          if (currentDb != null) {
            currentDb.close() // we need to close before restore
            currentDb = null
          }
          val restoreOptions = new RestoreOptions(false)
          backupEngine.restoreDbFromBackup(backupIdRecovery, localDbDir, localWalDataDir, restoreOptions)
          currentDb = openDb
        }
      }
      logDebug(s"restored db for $this version $version from local backup, took $tRestore secs")

      // delete potential newer backups to avoid diverging data versions. See also comment for [[BackupEngine.restoreDbFromBackup]]
      val newerBackups = backupList.filter{ case (v,(b,k)) => v > version}
      val tCleanup = MiscHelper.measureTime {
        remoteCleanupList.clear
        newerBackups.foreach{ case (v,(b,k)) =>
          Try(backupEngine.deleteBackup(b))
          backupList.remove(v)
          remoteCleanupList += new Path(remoteBackupPath,s"${getBackupKey(v)}.zip")
        }
        backupEngine.garbageCollect()

        // delete unused shared data files to avoid later conflicts
        val (_,sharedFiles2Del) = getRemoteSyncList(new Path(remoteBackupPath,"shared"), new Path(localBackupPath,"shared"), _ => true, true )
        if(sharedFiles2Del.nonEmpty) logInfo(s"found ${sharedFiles2Del.size} unsed remote files to delete for $this")
        remoteCleanupList ++= sharedFiles2Del
      }
      if (newerBackups.nonEmpty) logInfo(s"deleted newer local backups versions ${newerBackups.keys.toSeq.sorted.mkString(", ")} for $this, took $tCleanup secs")
    }
  }

  private def openDb: RocksDB = TtlDB.open(options, localDbDir, ttlSec, false)


  /**
    * Return the id of the StateStores this provider will generate.
    */
  override def stateStoreId: StateStoreId = stateStoreId_

  /**
    * Do maintenance backing data files, including cleaning up old files
    */
  override def doMaintenance(): Unit = try {
    logDebug(s"starting doMaintenance for $this")

    var sharedFilesSize: Long = 0
    val t = MiscHelper.measureTime {

      // flush WAL to SST Files and do manual compaction
      synchronized {
        val opened = if (currentDb==null) {
          currentDb = openDb
          true
        } else false
        currentDb.flush(new FlushOptions().setWaitForFlush(true))
        currentDb.pauseBackgroundWork()
        currentDb.compactRange()
        currentDb.continueBackgroundWork()
        if (opened) {
          currentDb.close()
          currentDb = null
        }
      }

      // estimate db size
      sharedFilesSize = localBackupFs.listStatus(new Path(localBackupPath,"shared"))
        .map(_.getLen).sum

      // cleanup old backups
      cleanupOldBackups()

      // remove cleaned up backups from remote filesystem and backup list
      val backupInfoVersions = backupEngine.getBackupInfo.asScala.map(_.appMetadata().toLong)
      val removedBackups = backupList.keys.filter( v => !backupInfoVersions.contains(v))
      if (!rotatingBackupKey) removedBackups
        .map( v => new Path(remoteBackupPath, s"${backupList(v)._2}.zip"))
        .foreach( f => remoteBackupFm.delete( f ))
      removedBackups.foreach(backupList.remove)
      syncRemoteIndex()
      logDebug(s"backup list for $this after doMaintenance: ${backupList.toSeq.sortBy(_._1).mkString(", ")}")

      // check free diskspace
      //val dirFreeSpace = Seq(localDbDir, localWalDataDir).distinct.map( f => s"$f=${new File(f).getUsableSpace().toFloat/1024/1024}MB")
      //logInfo(s"free disk space for this: "+dirFreeSpace.mkString(", "))
    }
    logInfo(s"doMaintenance for $this took $t secs, shared file size is ${MiscHelper.formatBytes(sharedFilesSize)}")
  } catch {
    case e:Exception =>
      logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'doMaintenance' of $this")
      throw e
  }

  /**
    * Called when the provider instance is unloaded from the executor.
    */
  override def close(): Unit = try {
    // cleanup
    if(currentDb!=null) {
      currentDb.close
      currentDb = null
    }
    FileUtils.deleteDirectory(new File(localDataDir))
    logInfo(s"Removed local db and backup dir of $this")
  } catch {
    case e:Exception =>
      logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'close' of $this")
  }

  /**
    * Custom toString implementation for this state store provider class.
    */
  override def toString: String = {
    s"RocksDbStateStoreProvider[op=${stateStoreId.operatorId},part=${stateStoreId.partitionId},dir=$remoteBackupPath]"
  }

  /**
    * creates a backup of the current database for the version given
    */
  private def createBackup(version: Long): Unit = {

    // create local backup
    val tBackup = MiscHelper.measureTime {
      // Don't flush before backup, as also WAL is backuped. We can use WAL for efficient incremental backup
      backupEngine.createNewBackupWithMetadata(currentDb, version.toString, false)
    }
    val backupId = backupEngine.getBackupInfo.asScala.map(_.backupId()).max
    logDebug(s"created backup for $this version $version with backupId $backupId, took $tBackup secs")

    // sync to remote filesystem
    var backupKey = ""
    val tSync = MiscHelper.measureTime {
      backupKey = syncRemoteBackup(backupId, version)
    }
    logDebug(s"synced $this version $version backupId $backupId to remote filesystem with backupKey $backupKey, took $tSync secs")
  }

  /**
    * copy local backup to remote filesystem
    */
  private def syncRemoteBackup(backupId: Int, version: Long): String = {

    // process remoteCleanupList before syncing new files
    remoteCleanupList.par.foreach( f =>
      remoteBackupFm.delete(f)
    )
    remoteCleanupList.clear

    // make sure backup is existing
    assert(backupEngine.getBackupInfo.asScala.exists(_.backupId() == backupId), s"backupId $backupId for $version not found")

    // diff shared data files
    val (sharedFiles2Copy,sharedFiles2Del) = getRemoteSyncList(new Path(remoteBackupPath,"shared"), new Path(localBackupPath,"shared"), _ => true, true )
    logDebug(s"found ${sharedFiles2Copy.size} files to copy to remote filesystem and ${sharedFiles2Del.size} files to delete for $this")

    // copy new data files in parallel
    sharedFiles2Copy.par.foreach( f =>
      copyLocalToRemoteFile(f, new Path(remoteBackupPath,s"shared/${f.getName}"), true)
    )

    // save metadata & private files as zip archive
    val backupKey = getBackupKey(version)
    val metadataFile = new File(s"$localBackupDir${File.separator}meta${File.separator}$backupId")
    val privateFiles = new File(s"$localBackupDir${File.separator}private${File.separator}$backupId").listFiles().toSeq
    MiscHelper.compress2Remote(metadataFile +: privateFiles, new Path(remoteBackupPath,s"$backupKey.zip"), remoteBackupFm, getHadoopFileBufferSize, Some(localBackupDir))

    // update index of backups
    backupList.find{ case (v,(b,k)) => k == backupKey}
      .foreach{ case (v,(b,k)) => backupList.remove(v)} // remove existing entry for this backup key
    backupList.put(version, (backupId,backupKey))
    syncRemoteIndex()

    // delete old data files in parallel
    sharedFiles2Del.par.foreach( f =>
      remoteBackupFm.delete(f)
    )

    //return
    backupKey
  }

  /**
    * get backup key for version
    * if rotatingBackupKey is set, this is a rotating key with minVersionsToRetain possible values
    * otherwise it's the version
    */
  private def getBackupKey(version: Long) = {
    if (rotatingBackupKey) MiscHelper.nbToChar((version % storeConf.minVersionsToRetain).toInt)
    else version.toString
  }

  /**
    * keep latest x backup versions, where x can be configured by minVersionsToRetain
    */
  private def cleanupOldBackups(): Unit = {
    val backupsSorted = backupEngine.getBackupInfo.asScala.toSeq.sortBy(_.appMetadata().toLong)
    val backupsToDelete = backupsSorted.take(backupsSorted.size-storeConf.minVersionsToRetain)
    backupsToDelete.foreach( b => backupEngine.deleteBackup(b.backupId()))
    backupEngine.garbageCollect()
    logDebug( s"backup engine contains the following backups for $this: " + backupEngine.getBackupInfo.asScala.map(b => s"v=${b.appMetadata()},b=${b.backupId}").mkString("; "))
  }

  /**
    * update index on remote filesystem
    */
  private def syncRemoteIndex(): Unit = {
    val indexOutputStream = new java.io.PrintStream(remoteBackupFm.createAtomic( new Path(remoteBackupPath,"index"), true))
    backupList.toSeq.sortBy(_._1)
      .foreach{ case (v,(b,k)) => indexOutputStream.println(s"version: $v, backupId: $b, backupKey: $k")}
    indexOutputStream.close()
  }

  /**
    * compares the content of a local directory with a remote directory and returns list of files to copy and list of files to delete
    */
  private def getRemoteSyncList( remotePath: Path, localPath: Path, nameFilter: String => Boolean, errorOnChange: Boolean ) = {
    val remoteFiles = remoteBackupFm.list(remotePath, new PathFilter {
      override def accept(path: Path) = nameFilter(path.getName)
    }).toSeq
    val localFiles = localBackupFs.listStatus(localPath, new PathFilter {
      override def accept(path: Path) = nameFilter(path.getName)
    }).toSeq

    // find local files not existing or different in remote dir
    val files2Copy = localFiles.flatMap( lf =>
      remoteFiles.find( rf => rf.getPath.getName==lf.getPath.getName ) match {
        case None => Some(lf) // not existing -> copy
        case Some(rf) if rf.getLen!=lf.getLen => // existing and different -> copy
          if (errorOnChange) logError(s"Remote file ${rf.getPath} is different from local file ${lf.getPath}. Overwriting for now.")
          Some(lf)
        case _ => None // existing and the same -> nothing
      }
    ).map(_.getPath)

    // delete remote files not existing in local dir
    val files2Del = remoteFiles.filter( rf => !localFiles.exists( lf => lf.getPath.getName==rf.getPath.getName ))
      .map(_.getPath)

    (files2Copy,files2Del)
  }

  /**
    * reads archive files on remote filesystem and tries to reconstruct the index file
    * this is only needed if index file got corrupted on remote filesystem
    */
  private def getBackupListFromRemoteFiles = {
    // search and delete remote backup
    val archiveFiles = remoteBackupFm.list(remoteBackupPath, new PathFilter {
      override def accept(path: Path): Boolean = path.getName.endsWith(".zip")
    })
    archiveFiles.map { f =>
      val backupKey = f.getPath.getName.takeWhile(_ != '.')
      val input = new ZipInputStream(remoteBackupFm.open(f.getPath))
      val metaFileEntry = Iterator.continually(input.getNextEntry)
        .takeWhile(_ != null)
        .find(entry => entry.getName.matches(".*meta/[0-9]*"))
        .getOrElse(throw new IllegalStateException(s"Couldn't find meta file in archive ${f.getPath}"))
      val backupId = metaFileEntry.getName.reverse.takeWhile(_ != '/').reverse.toInt
      // read metafile
      val metaBuffer = new Array[Byte](1000) // metafile shouldn't be bigger than that...
      input.read(metaBuffer)
      val metaLines = new String(metaBuffer).lines.toSeq
      val metaLineIdx = metaLines.indexWhere(_.startsWith("metadata"))
      val version = metaLines(metaLineIdx + 1).toLong
      input.close()
      (version, (backupId,backupKey))
    }.toMap
  }

  private def getHadoopFileBufferSize = hadoopConf.getInt("io.file.buffer.size", 4096)

  def copyLocalToRemoteFile( src: Path, dst: Path, overwriteIfPossible: Boolean ): Unit = {
    org.apache.hadoop.io.IOUtils.copyBytes( localBackupFs.open(src), remoteBackupFm.createAtomic(dst, overwriteIfPossible), hadoopConf, true)
  }

  def copyRemoteToLocalFile( src: Path, dst: Path, overwriteIfPossible: Boolean ): Unit = {
    org.apache.hadoop.io.IOUtils.copyBytes( remoteBackupFm.open(src), localBackupFs.create(dst, overwriteIfPossible), hadoopConf, true)
  }

  /**
    * Get name for local data directory.
    */
  private def getDataDirName(sparkJobName: String): String = {
    // we need a random number to allow multiple streaming queries running with different state stores in the same spark job.
    s"spark-$sparkJobName-${stateStoreId_.operatorId}-${stateStoreId_.partitionId}-${stateStoreId_.storeName}-${math.abs(Random.nextInt)}"
  }

  /**
    * Get iterator of all the data of the latest version of the store.
    */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = {
    val maxVersion = backupList.keys.max
    getStore(maxVersion).iterator()
  }

}

/**
  * Companion object with constants.
  */
object RocksDbStateStoreProvider {
  type MapType = com.google.common.cache.LoadingCache[UnsafeRow, String]

  /** Default write buffer size for RocksDb in megabytes */
  final val WRITE_BUFFER_SIZE_MB: String = "spark.sql.streaming.stateStore.writeBufferSizeMb"

  val DEFAULT_WRITE_BUFFER_SIZE_MB = 64

  /** Default write buffer number for RocksDb: as we normally close Rocksdb after each commit (see also closeDbAfterCommit),
    * multiple write buffers don't make sense as each key of the spark partition is read/written at most once */
  final val WRITE_BUFFER_NUMBER: String = "spark.sql.streaming.stateStore.writeBufferSizeMb"

  val DEFAULT_WRITE_BUFFER_NUMBER = 1

  /** Default block cache size for RocksDb in megabytes */
  final val BLOCK_CACHE_SIZE_MB: String = "spark.sql.streaming.stateStore.blockCacheSizeMb"

  val DEFAULT_BLOCK_CACHE_SIZE_MB = 128

  /** statistics */
  val ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY = "rocksdb.estimate-num-keys"
  val ROCKSDB_ESTIMATE_TABLE_READERS_MEM = "rocksdb.estimate-table-readers-mem"
  val ROCKSDB_SIZE_ALL_MEM_TABLES = "rocksdb.size-all-mem-tables"
  val ROCKSDB_CUR_SIZE_ALL_MEM_TABLES = "rocksdb.cur-size-all-mem-tables"
  val ROCKSDB_VOLUME_PROPERTIES = Seq(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY,ROCKSDB_ESTIMATE_TABLE_READERS_MEM,ROCKSDB_SIZE_ALL_MEM_TABLES,ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)

  final val STATE_EXPIRY_SECS: String = "spark.sql.streaming.stateStore.stateExpirySecs"

  final val DEFAULT_STATE_EXPIRY_SECS: String = "-1"

  final val STATE_EXPIRY_STRICT_MODE: String = "spark.sql.streaming.stateStore.strictExpire"

  final val DEFAULT_STATE_EXPIRY_METHOD: String = "false"

  final val STATE_LOCAL_DIR: String = "spark.sql.streaming.stateStore.localDir"

  final val DEFAULT_STATE_LOCAL_DIR: String = System.getProperty("java.io.tmpdir").replace('\\','/')

  final val STATE_LOCAL_WAL_DIR: String = "spark.sql.streaming.stateStore.localWalDir"

  final val DEFAULT_STATE_LOCAL_WAL_DIR: String = DEFAULT_STATE_LOCAL_DIR

  final val STATE_ROTATING_BACKUP_KEYS: String = "spark.sql.streaming.stateStore.rotatingBackupKeys"

  final val DEFAULT_STATE_ROTATING_BACKUP_KEYS: String = "false"

  final val STATE_COMPRESSION_TYPE: String = "spark.sql.streaming.stateStore.compressionType"

  final val DEFAULT_STATE_COMPRESSION_TYPE: String = null // NO_COMPRESSION

  final val STATE_CLOSE_DB_AFTER_COMMIT: String = "spark.sql.streaming.stateStore.closeDbAfterCommit"

  final val DEFAULT_STATE_CLOSE_DB_AFTER_COMMIT: String = "true"

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

  private def setWriteBufferSizeMb(conf: Map[String, String]): Int =
    Try(conf.getOrElse(WRITE_BUFFER_SIZE_MB, DEFAULT_WRITE_BUFFER_SIZE_MB.toString).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setWriteBufferNumber(conf: Map[String, String]): Int =
    Try(conf.getOrElse(WRITE_BUFFER_NUMBER, DEFAULT_WRITE_BUFFER_NUMBER.toString).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setBlockCacheSizeMb(conf: Map[String, String]): Int =
    Try(conf.getOrElse(BLOCK_CACHE_SIZE_MB, DEFAULT_BLOCK_CACHE_SIZE_MB.toString).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
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

  private def setLocalDir(conf: Map[String, String]): String =
    Try(conf.getOrElse(STATE_LOCAL_DIR, DEFAULT_STATE_LOCAL_DIR)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setLocalWalDir(conf: Map[String, String]): String =
    Try(conf.getOrElse(STATE_LOCAL_WAL_DIR, DEFAULT_STATE_LOCAL_WAL_DIR)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setRotatingBackupKey(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_ROTATING_BACKUP_KEYS, DEFAULT_STATE_ROTATING_BACKUP_KEYS).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setCompressionType(conf: Map[String, String]): CompressionType =
    Try(CompressionType.getCompressionType(conf.getOrElse(STATE_COMPRESSION_TYPE, DEFAULT_STATE_COMPRESSION_TYPE))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setCloseDbAfterCommit(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_CLOSE_DB_AFTER_COMMIT, DEFAULT_STATE_CLOSE_DB_AFTER_COMMIT).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

}

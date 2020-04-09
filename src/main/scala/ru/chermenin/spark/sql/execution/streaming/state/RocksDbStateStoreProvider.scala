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

import java.io._
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SchemaHelper, functions}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.{DataType, StructType}
import org.rocksdb.{TickerType, _}
import org.rocksdb.util.SizeUnit
import ru.chermenin.spark.sql.execution.streaming.state.MiscHelper.using

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

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
  * for >= ttl amount of time and the db will make efforts to remove the key-values
  * as soon as possible after ttl seconds of their insertion.
  *  - In strict mode, the key-values inserted will remain in the db for exactly ttl amount of time.
  * To ensure exact expiration, a separate cache of keys is maintained in memory with
  * their respective deadlines and is used for reference during operations.
  *
  * The timeouts may be set on global (for all queries) for differently for each streaming query.
  * This can be done be appending the query name to [[STATE_EXPIRY_SECS]], like below,
  *
  * spark.sql.streaming.stateStore.stateExpirySecs.queryName1 = 5
  *
  * This API can also be used to allow,
  *  - Stateless Processing - set timeout to 0
  *  - Infinite State (no timeout) - set timeout to -1, which is set by default.
  *
  * Notes
  * -----
  * - Attention: On windows deletion of Files which are written over java.nio is not possible in current java process.
  * This is an old bug which might be solved in Java 10 or 11, see https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
  *
  *  TODO: What happens when backupId overflows (backupId is an integer, but version is long)
  */
class RocksDbStateStoreProvider extends StateStoreProvider with Logging {
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._ // import companion object

  /** Load native RocksDb library */
  RocksDB.loadLibrary()

  private var options: Options = _
  private var writeOptions: WriteOptions = _
  private var backupDBOptions: BackupableDBOptions = _

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
    override def get(key: UnsafeRow): UnsafeRow = RocksDbStateStoreProvider.this.synchronized {
      try {
        MiscHelper.verify(currentDb != null, "RocksDb must be opened")

        var returnValue: UnsafeRow = null
        val t = MiscHelper.measureTime {
          returnValue = if (isStrictExpire) {
            Option(keyCache.getIfPresent(key)) match {
              case Some(_) => getValue(key)
              case None => null
            }
          } else getValue(key)
        }
        val returnValueLog = if (returnValue != null) "result size is " + returnValue.getSizeInBytes else "no value found"
        logDebug(s"get ${key.toSeq(keySchema)} took $t secs, $returnValueLog")
        returnValue
      } catch {
        case e: Exception =>
          logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'get' key ${key.toSeq(keySchema)} of $this")
          throw e
      }
    }

    /**
      * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
      * the params can be reused, and must make copies of the data as needed for persistence.
      */
    override def put(key: UnsafeRow, value: UnsafeRow): Unit = RocksDbStateStoreProvider.this.synchronized {
      try {
        MiscHelper.verify(state == State.Updating, "Cannot put entry into already committed or aborted state")
        MiscHelper.verify(currentDb != null, "RocksDb must be opened")

        val t = MiscHelper.measureTime {
          val keyCopy = key.copy()
          val valueCopy = value.copy()
          currentDb.put(writeOptions, keyCopy.getBytes, valueCopy.getBytes)

          if (isStrictExpire) keyCache.put(keyCopy, DUMMY_VALUE)
        }
        logDebug(s"put ${key.toSeq(keySchema)} took $t secs, size is ${value.getSizeInBytes}")
      } catch {
        case e:Exception =>
          logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'put' key ${key.toSeq(keySchema)} of $this")
          throw e
      }
    }

    /**
      * Remove a single non-null key.
      */
    override def remove(key: UnsafeRow): Unit = RocksDbStateStoreProvider.this.synchronized {
      try {
        MiscHelper.verify(state == State.Updating, "Cannot remove entry from already committed or aborted state")
        MiscHelper.verify(currentDb != null, "RocksDb must be opened")

        currentDb.delete(key.getBytes)
        if (isStrictExpire) keyCache.invalidate(key.getBytes)
      } catch {
        case e:Exception =>
          logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'remove' of $this")
          throw e
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
    override def getRange(start: Option[UnsafeRow], end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = RocksDbStateStoreProvider.this.synchronized {
      MiscHelper.verify(state == State.Updating, "Cannot getRange from already committed or aborted state")
      iterator()
    }

    /**
      * Commit all the updates that have been made to the store
      * Execute state maintenance if embedded mode is enabled
      *
      * Return the new version number.
      */
    override def commit(): Long = RocksDbStateStoreProvider.this.synchronized  {
      try {
        MiscHelper.verify(state == State.Updating, "Cannot commit already committed or aborted state")
        MiscHelper.verify(currentDb != null, "RocksDb must be opened")

        if (triggerMaintenance) {
          execStateMaintenance()
          triggerMaintenance = false
        }

        updateStatistics()

        state = State.Committed
        createBackup(newVersion)
        if (closeDbOnCommit) closeDb()

        logInfo(s"Committed version $newVersion for $this")
        newVersion

      } catch {
        case e: Exception =>
          logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'commit' for version $newVersion of $this")
          throw e
      }
    }

    def updateStatistics(): Unit = {
      // log statistics
      val statsTypes = Seq(
          TickerType.NUMBER_KEYS_READ, TickerType.NUMBER_KEYS_UPDATED, TickerType.NUMBER_KEYS_WRITTEN
        , TickerType.MEMTABLE_HIT, TickerType.MEMTABLE_MISS
        , TickerType.BLOCK_CACHE_HIT, TickerType.BLOCK_CACHE_MISS
        , TickerType.BYTES_READ, TickerType.BYTES_WRITTEN
      )
      logDebug(s"rocksdb cache statistics for $this: "+statsTypes.map( t => s"$t=${currentDbStats.getAndResetTickerCount(t)}" ).mkString(" "))

      //log volume statistics
      rocksdbVolumeStatistics = ROCKSDB_VOLUME_PROPERTIES.map{ p =>
        if (p==ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY && isStrictExpire) p -> keyCache.size
        else p -> (if (currentDb!=null) currentDb.getLongProperty(p) else -1l)
      }.toMap
      logDebug(s"rocksdb volume statistics for $this: "+rocksdbVolumeStatistics.map{ case (k,v) => s"$k=$v"}.mkString(" "))
    }

    /**
      * Abort all the updates made on this store. This store will not be usable any more.
      */
    override def abort(): Unit = RocksDbStateStoreProvider.this.synchronized {
      try {
        MiscHelper.verify(state != State.Committed, "Cannot abort already committed state")
        //TODO: how can we rollback uncommitted changes -> we should use a transaction!

        state = State.Aborted
        if (closeDbOnCommit) closeDb()

        logInfo(s"Aborted version $newVersion for $this")

      } catch {
        case e: Exception =>
          logWarning(s"Error aborting version $newVersion into $this", e)
      }
    }

    /**
      * Get an iterator of all the store data.
      */
    override def iterator(): Iterator[UnsafeRowPair] = RocksDbStateStoreProvider.this.synchronized {
      val openedDb = if (currentDb == null) {
        currentDb = openDb
        true
      } else false

      val stateFromRocksIter = RocksDbHelper.getIterator(currentDb, keySchema, valueSchema)
      val stateIter = {
        keyCache.cleanUp()
        val keys = keyCache.asMap().keySet()

        if (isStrictExpire) stateFromRocksIter.filter(x => keys.contains(x.key))
        else stateFromRocksIter
      }

      if (openedDb) closeDb()

      stateIter
    }

    /**
      * Returns current metrics of the state store
      */
    override def metrics: StateStoreMetrics = {
      val estimatedKeyNb: Long = rocksdbVolumeStatistics.getOrElse(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY, 0)
      val memTableSize: Long = rocksdbVolumeStatistics.getOrElse(ROCKSDB_SIZE_ALL_MEM_TABLES, 0)
      StateStoreMetrics( estimatedKeyNb, memTableSize + rocksDbCurrentSize, Map.empty)
    }

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

  // case class to store informations about remote backups
  case class RemoteBackupInfo(version: Long, tstamp: Long, path: Path, altPath: Seq[Path] = Seq(), backupId: Option[Int] = None, sharedFiles: Seq[Path] = Seq())

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
  @volatile private var closeDbOnCommit: Boolean = _
  @volatile private var manualRocksDbCompaction: Boolean = _
  @volatile private var embeddedMaintenance: Boolean = _
  @volatile private var remoteUploadRetries: Int = 1
  @volatile private var rocksDbBackupSize: Long = 0 // store backup size metric
  @volatile private var rocksDbCurrentSize: Long = 0 // store backup size metric
  @volatile private var removeExpiredRowsInMaintenance: Boolean = _
  @volatile private var removeExpiredRowsInMaintenanceColName: String = _
  @volatile private var cleanupRemoteBackups: Boolean = _
  @volatile private var minRemoteVersionsToRetain: Int = 1
  @volatile private var minLocalBackupsToRetain: Int = 1
  @volatile private var loadRemoteBackupSelective: Boolean = _
  private var remoteBackupPath: Path = _
  private var remoteBackupFs: FileSystem = _
  private var localBackupPath: Path = _
  private var localBackupSharedPath: Path = _
  private var localBackupFs: FileSystem = _
  private var localBackupDir: String = _
  private var localDbDir: String = _
  private var currentDb: RocksDB = _
  private val currentDbStats: Statistics = new Statistics()
  private var currentStore: RocksDbStateStore = _
  private var remoteBackupFm: CheckpointFileManager = _
  private var remoteBackupSchemaPath: Path = _
  private var remoteBackupSharedPath: Path = _
  private var backupSchemas: Seq[(Long,String,StructType)] = Seq()
  private var backupKeySchemaUpToDate = false
  private var backupValueSchemaUpToDate = false
  private var triggerMaintenance = false
  private var remoteBackupInfos: mutable.Map[Long,RemoteBackupInfo] = mutable.Map()
  @volatile private var expirationByQuery: Map[String, Int] = _
  @volatile private var actualCheckpointRoot: String = _
  @volatile private var queryName: String = _

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
                    hadoopConf: Configuration): Unit = synchronized {
    try {
      this.stateStoreId_ = stateStoreId
      this.keySchema = keySchema
      this.valueSchema = valueSchema
      this.storeConf = storeConf
      this.hadoopConf = hadoopConf
      this.localDataDir = MiscHelper.createLocalDir( setLocalDir(storeConf.confs)+"/"+getDataDirName(Option(hadoopConf.get("spark.app.name"))))
      this.localWalDataDir = MiscHelper.createLocalDir( setLocalWalDir(storeConf.confs)+"/"+getDataDirName(Option(hadoopConf.get("spark.app.name"))))
      this.ttlSec = setTTL(storeConf.confs)
      this.isStrictExpire = setExpireMode(storeConf.confs)
      this.closeDbOnCommit = setCloseDbAfterCommit(storeConf.confs)
      this.manualRocksDbCompaction = setManualRocksDbCompaction(storeConf.confs)
      this.embeddedMaintenance = setEmbeddedMaintenance(storeConf.confs)
      this.remoteUploadRetries = setRemoteUploadRetries(storeConf.confs)
      this.removeExpiredRowsInMaintenance = setRemoveExpiredRowsInMaintenance(storeConf.confs)
      this.removeExpiredRowsInMaintenanceColName = setRemoveExpiredRowsInMaintenanceColName(storeConf.confs)
      this.cleanupRemoteBackups = setCleanupRemoteBackups(storeConf.confs)
      this.minRemoteVersionsToRetain = storeConf.minVersionsToRetain
      this.minLocalBackupsToRetain = setMinLocalBackupsToRetain(storeConf.confs, storeConf.minVersionsToRetain)
      this.loadRemoteBackupSelective = setLoadRemoteBackupSelective(storeConf.confs)
      assert( cleanupRemoteBackups || loadRemoteBackupSelective, "loadRemoteBackupSelective must be enabled when cleanupRemoteBackup is disabled for performance reasons")
      assert( minLocalBackupsToRetain <= 100, "if minVersionToRetain is > 100, minLocalBackupsToRetain must be set to limit local space requirements")
      assert( minLocalBackupsToRetain <= minRemoteVersionsToRetain )

      // initialize paths
      remoteBackupPath = stateStoreId.storeCheckpointLocation()
      remoteBackupFs = remoteBackupPath.getFileSystem(hadoopConf)
      localBackupDir = MiscHelper.createLocalDir(localDataDir+"/backup")
      localBackupPath = new Path("file:///"+localBackupDir)
      localBackupSharedPath = new Path( localBackupPath, "shared_checksum" )
      localBackupFs = localBackupPath.getFileSystem(hadoopConf)
      localDbDir = MiscHelper.createLocalDir(localDataDir+"/db")
      remoteBackupFm = CheckpointFileManager.create(remoteBackupPath, hadoopConf)
      remoteBackupSchemaPath = new Path(remoteBackupPath, "schema")
      remoteBackupSharedPath = new Path(remoteBackupPath, "shared")

      // initialize new RocksDbLogger with Error level for now
      val rocksDbLogger = new org.rocksdb.Logger(new Options().setInfoLogLevel(InfoLogLevel.ERROR_LEVEL)) {
        def log( logLevel: InfoLogLevel, logMsg: String ): Unit = {
          if (logLevel == InfoLogLevel.ERROR_LEVEL) logError(logMsg)
          else if (logLevel == InfoLogLevel.WARN_LEVEL) logWarning(logMsg)
          else if (logLevel == InfoLogLevel.INFO_LEVEL) logInfo(logMsg)
          else logDebug(logMsg)
        }
      }

      // initialize empty database
      currentDbStats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS)
      options = new Options()
        .setLogger(rocksDbLogger) // set custom logger
        .setStatistics(currentDbStats)
        .setCreateIfMissing(true)
        .setWriteBufferSize(setWriteBufferSizeMb(storeConf.confs) * SizeUnit.MB)
        .setMaxWriteBufferNumber( setWriteBufferNumber(storeConf.confs))
        // .setDbWriteBufferSize() // this is the same as setWriteBufferSize*setMaxWriteBufferNumber
        .setAllowMmapReads(false) // could be responsible for memory leaks according to https://github.com/facebook/rocksdb/issues/3216
        .setAllowMmapWrites(false) // this could make memory leaks according to https://github.com/facebook/rocksdb/issues/3216
        //.setTargetFileSizeBase(xxx * SizeUnit.MB))
        //.setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(false).setBlockCacheSize(setBlockCacheSizeMb(storeConf.confs) * SizeUnit.MB))
        //.setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true))
        .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(getGlobalBlockCache(setBlockCacheSizeMb(storeConf.confs) * SizeUnit.MB)))
        .setDisableAutoCompactions( manualRocksDbCompaction ) // if auto compactions is disabled, we trigger manual compaction during state maintenance with compactRange()
        .setWalDir(localWalDataDir) // Write-ahead-log should be saved on fast disk (local, not NAS...)
        .setCompressionType(setCompressionType(storeConf.confs))
        .setAvoidFlushDuringRecovery(true) // Dont flush when opening database
        //.setCompactionStyle(CompactionStyle.UNIVERSAL) // use default normal Leveled compaction for now
        //.setParanoidChecks(false) // use default for now
        //.setForceConsistencyChecks(false) // use default for now
        //.setParanoidFileChecks(false) // use default for now
      writeOptions = new WriteOptions()
        .setDisableWAL(false) // we use write ahead log for efficient incremental state versioning
        .setSync(true)
      backupDBOptions = new BackupableDBOptions(localBackupDir.toString)
        //.setInfoLog(rocksDbLogger) // this crashes
        .setShareTableFiles(true)
        .setShareFilesWithChecksum(true)
        .setBackupLogFiles(true)
        .setSync(true)

      // restore backups from remote to local directory
      restoreFromRemoteBackups()

      // read and check state schemas
      val backupSchemaFiles = getRemoteBackupSchemaFiles
      val latestVersion = Try(remoteBackupInfos.keys.max).getOrElse(-1l)
      backupSchemas = backupSchemaFiles.flatMap( parseBackupSchemaFile(latestVersion))
      val latestBackupKeySchema = getBackupKeySchema(latestVersion)
      val latestBackupValueSchema = getBackupValueSchema(latestVersion)
      if (latestBackupKeySchema.isEmpty) logWarning(s"latest backup key schema not found (version=$latestVersion)")
      else if (keySchema!=null && latestBackupKeySchema.get!=keySchema) logWarning(s"latest backup key schema is different from current schema: latestBackup=${latestBackupKeySchema.get.json}, current=${keySchema.json}")
      if (latestBackupValueSchema.isEmpty) logWarning(s"latest backup value schema not found (version=$latestVersion)")
      else if (valueSchema!=null && latestBackupValueSchema.get!=valueSchema) logWarning(s"latest backup value schema is different from current schema: latestBackup=${latestBackupValueSchema.get.json}, current=${valueSchema.json}")

      logInfo(s"initialized $this, localDataDir=$localDataDir, localWalDataDir=$localWalDataDir, cleanupRemoteBackups=$cleanupRemoteBackups, minRemoteVersionsToRetain=$minRemoteVersionsToRetain, minLocalBackupsToRetain=$minLocalBackupsToRetain")
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'init' of $this")
        throw e
    }
  }

  protected def restoreFromRemoteBackups(): Unit = {
    // copy backups from remote storage and init backup engine
    if (remoteBackupFm.exists(remoteBackupPath)) {
      logDebug(s"loading state backup with loadRemoteBackupSelective=$loadRemoteBackupSelective from remote filesystem $remoteBackupPath for $this")

      // cleanup possible existing local backup files
      // Note: deleting localBackupDir on Windows results in permission errors in unit tests... we do the cleanup only for linux for now
      if (!MiscHelper.isWindowsOS) {
        FileUtils.deleteQuietly(new File(localBackupDir))
      }

      // load files
      val t = MiscHelper.measureTime {

        // list & remember remote backups
        remoteBackupInfos.clear
        remoteBackupInfos ++= getLatestRemoteBackupInfos

        // create local directories
        localBackupFs.mkdirs(localBackupSharedPath)

        // copy all backup files and extract meta/private files in parallel if not selective restore
        if(!loadRemoteBackupSelective || cleanupRemoteBackups) {
          val remoteBackupInfosToRestore = if (cleanupRemoteBackups) {
            remoteBackupInfos.values.toSeq.sortBy(_.version).reverse.take(minRemoteVersionsToRetain)
          } else {
            remoteBackupInfos.values.toSeq.sortBy(_.version).reverse.take(minLocalBackupsToRetain)
          }
          // load remote backup files and overwrite remoteBackupInfos with additional informations
          loadRemoteBackupFiles(remoteBackupInfosToRestore).foreach { backupInfo =>
            remoteBackupInfos.put( backupInfo.version, backupInfo )
          }
        }

        // copy all shared files in parallel if not selective restore
        if(!loadRemoteBackupSelective) try {
          val filesToLoad = remoteBackupInfos.values.flatMap(_.sharedFiles).toSeq.distinct
          loadRemoteSharedFiles(filesToLoad, true)
        } catch {
          case e: FileNotFoundException => logInfo(s"$remoteBackupSharedPath doesn't yet exist. It will be created on commit.")
        }
      }
      logInfo(s"got state backup from remote filesystem $remoteBackupPath for $this, took $t secs, existing backups versions are $getBackupInfoVersionStr with loadRemoteBackupSelective=$loadRemoteBackupSelective")

    } else {
      logDebug(s"initializing state backup at $remoteBackupPath for $this")
      remoteBackupFm.mkdirs(remoteBackupSharedPath)
      remoteBackupFm.mkdirs(remoteBackupSchemaPath)
      currentDb = openDb
      createBackup(0L)
      closeDb()
    }
  }

  def getRemoteBackupInfos: Seq[RemoteBackupInfo] = {
    remoteBackupFm.list(remoteBackupPath).toSeq
      .filter( f => !f.isDirectory && f.getPath.getName.matches("[0-9_\\.]*.zip"))
      .map{ f => // extract version and timestamp
        // filename format is "<version>_<tstampMs>.zip" or "<version>_<contextId>_<tstampMs>.zip"
        val parsedName = f.getPath.getName.stripSuffix(".zip").split('_')
        assert(parsedName.length >= 2, s"Filename format is strange for ${f.getPath}")
        val version = parsedName.head.toLong
        val tstamp = parsedName.last.toLong
        RemoteBackupInfo(version, tstamp, f.getPath)
      }
  }

  def getLatestRemoteBackupInfos: Map[Long,RemoteBackupInfo] = {
    getRemoteBackupInfos
      .groupBy( _.version )
      .mapValues( backupInfos => {
        val sortedBackupInfos = backupInfos.sortBy(_.tstamp).reverse
        // enrich latest backup info with path from earlier backup infos if existing
        sortedBackupInfos.head.copy( altPath = Try(sortedBackupInfos.tail).getOrElse(Seq()).map(_.path))
      }) // get entry with latest timestamp per group
  }

  def getBackupInfoVersionStr: String = remoteBackupInfos.keys.toSeq.sorted.mkString(", ")

  def getBackupInfo(backupEngine: BackupEngine): Map[Long,Int] = backupEngine.getBackupInfo.asScala.map( b => (b.appMetadata.toLong, b.backupId())).toMap
  def getBackupInfoStr(backupEngine: BackupEngine): String = getBackupInfo(backupEngine).toSeq.sortBy(_._1).map( i => s"v=${i._1},b=${i._2}").mkString("; ")

  private def loadRemoteBackupFiles( backupInfos: Seq[RemoteBackupInfo] ) = {
    backupInfos.par
      .flatMap { backupInfo =>
        try {
          // extract files from backup
          val filesCreated = MiscHelper.extractFromRemote(backupInfo.path, localBackupDir, remoteBackupFm, getHadoopFileBufferSize)
          // search for backupId in path
          val unixOrWinSeparator = "[\\\\/]"
          val backupIdRegex = (s""".*${unixOrWinSeparator}meta${unixOrWinSeparator}([0-9]*)""").r
          val backupId = filesCreated
            .collectFirst{
              case backupIdRegex(backupId) => backupId.toInt
            }.getOrElse( throw new IllegalStateException(s"""Could not find meta file in extracted files ${filesCreated.mkString(", ")}"""))
          // get list of shared files needed for this backup
          val remoteSharedFiles = RocksDbHelper.parseSharedFilesFromMetadata(localBackupDir, backupId).map( f => new Path(remoteBackupSharedPath, f))
          // return backup infos
          Some(backupInfo.copy(backupId = Some(backupId), sharedFiles = remoteSharedFiles))
        } catch {
          case e: FileNotFoundException =>
            logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' when copying metadata/private files from remote backup ${backupInfo.path.getName}")
            None
        }
    }.seq
  }

  private def loadRemoteSharedFiles(paths: Seq[Path], tolerantIfNotExists: Boolean ) = {
    paths.par
      .foreach(p => try {
        // shared files with checksum must be copied into a different folder
        copyRemoteToLocalFile(p, new Path(localBackupSharedPath, p.getName), true)
      } catch {
        // This is to be tolerant in case of inconsistent S3 metadata: the object might be deleted but it's still listed in state
        // In this case we can catch the FileNotFoundException, log it as error and do not fail the job.
        // This is no problem for state consistency, as the problematic file should have been deleted. It's no longer needed.
        case e: FileNotFoundException =>
          if (tolerantIfNotExists) logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' when copying shared remote backup file ${p.getName}. This is an inconsistency between S3 object and metadata. Please cleanup manually the no longer existing file.")
          else throw e
      })
  }

  /**
    * This is used for external inspection:
    * trigger a refresh of the database
    */
  def refreshDb(): RocksDB = {
    closeDb()
    currentDb = openDb
    currentDb
  }

  /**
    * open a backup engine instance
    * please close if no longer needed!
    */
  def createBackupEngine: BackupEngine = {
    BackupEngine.open(options.getEnv, backupDBOptions)
  }

  /**
    * Get the state store for making updates to create a new `version` of the store.
    */
  override def getStore(version: Long): StateStore = synchronized {
    try {
      getStore(version, createCache(ttlSec))
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'getStore' of $this for version $version")
        throw e
    }
  }
  protected[state] def getStore(version: Long, cache: MapType): StateStore = {
    require(version >= 0, "Version cannot be less than 0")

    // try restoring remote backups if version doesn't exist in local backups. This might happen if execution of a partition changes executor.
    val remoteBackupInfo = remoteBackupInfos.get(version)
    if (remoteBackupInfo.isEmpty) {
      restoreFromRemoteBackups()
      val remoteBackupInfoRetry = remoteBackupInfos.get(version)
      if (remoteBackupInfoRetry.isEmpty) throw new IllegalStateException(s"Can not find version $version in remote backup versions ($getBackupInfoVersionStr)")
    }
    val t = MiscHelper.measureTime {
      restoreAndOpenDb(version)
      // when exporting state, key/value Schema is set to null and we need to use the backup key/value schemas
      currentStore = new RocksDbStateStore(version
        , Option(keySchema).orElse(getBackupKeySchema(version)).get
        , Option(valueSchema).orElse(getBackupValueSchema(version)).get, cache)
    }
    logInfo(s"Retrieved $currentStore for $this version $version with loadRemoteBackupSelective=$loadRemoteBackupSelective, took $t seconds")
    // return
    currentStore
  }

  protected def restoreAndOpenDb(version: Long): Unit = {
    logDebug(s"starting to restore db for $this version $version from local backup")

    // check if current db is desired version, else search in backups and restore
    if (currentStore!=null && currentStore.getCommittedVersion.contains(version)) {
      if (currentDb==null) {
        currentDb = openDb
      }
      logDebug(s"Current db already has correct version $version. No restore required for $this.")
    } else {

      // get backup info
      val remoteBackupInfoLookup = remoteBackupInfos.getOrElse(version, throw new IllegalStateException(s"backup for version $version not found in remote backup infos ${getBackupInfoVersionStr}"))

      // copy metadata/private file if needed (selective restore)
      val remoteBackupInfoLoaded = if(remoteBackupInfoLookup.backupId.isEmpty) {
        // copy metadata/private file
        val remoteBackupInfoLoaded = loadRemoteBackupFiles(Seq(remoteBackupInfoLookup)).head
        remoteBackupInfos.put(remoteBackupInfoLoaded.version, remoteBackupInfoLoaded)
        remoteBackupInfoLoaded
      } else remoteBackupInfoLookup

      // copy shared files needed
      val existingLocalSharedFiles = localBackupFs.listStatus(localBackupSharedPath).map(_.getPath.getName)
      val sharedFilesToCopy = remoteBackupInfoLoaded.sharedFiles.map(_.getName).diff(existingLocalSharedFiles)
      loadRemoteSharedFiles(sharedFilesToCopy.map( sharedFileName => new Path(remoteBackupSharedPath, sharedFileName)), false)

      // get infos about backup to recover
      val backupEngine = createBackupEngine
      val backupIdRecovery = getBackupInfo(backupEngine)
        .getOrElse( version, throw new IllegalStateException(s"backup for version $version not found in backup engine"))

      // close db and restore backup
      val tRestore = MiscHelper.measureTime {
        closeDb() // we need to close before restore
        val restoreOptions = new RestoreOptions(false)
        backupEngine.restoreDbFromBackup(backupIdRecovery, localDbDir, localWalDataDir, restoreOptions)
        restoreOptions.close()
        backupEngine.close()
        currentDb = openDb
      }
      logDebug(s"restored db for $this version $version from local backup, took $tRestore secs")

      // check key schema
      val backupKeySchema = getBackupKeySchema(version)
      if (backupKeySchema.isDefined && keySchema!=null && backupKeySchema.get!=keySchema) throw new IllegalStateException(s"backup key schema not compatible with current schema. Schema Evolution for key schema is not rational. backup: ${backupKeySchema.get.json}, current: ${keySchema.json}")

      // check value schema and try schema evolution
      val backupValueSchema = getBackupValueSchema(version)
      if (backupValueSchema.isDefined && valueSchema!=null && backupValueSchema.get!=valueSchema) {
        // create schema evolution projection
        val defaultValues = storeConf.confs.filterKeys(_.startsWith(STATE_SCHEMAEVOLUTION_DEFAULTVALUE_PREFIX))
          .map{ case (k,v) => (k.stripPrefix(STATE_SCHEMAEVOLUTION_DEFAULTVALUE_PREFIX+"."), functions.expr(v))}
        val projection = SchemaHelper.getSchemaProjection(backupValueSchema.get, valueSchema, defaultValues)
        logInfo( s"value schema evolution needed for $this. Projection: $projection" )
        // migrate all records
        var recordCnt = 0
        val tMigration = MiscHelper.measureTime {
          val allKVIter = RocksDbHelper.getIterator(currentDb, backupKeySchema.get, backupValueSchema.get)
          val unsafeConverter = UnsafeProjection.create(valueSchema) // InternalRow -> UnsafeRow
          allKVIter.foreach {
            unsafeRowPair =>
              val newValueRow = SchemaHelper.applySchemaProjection(unsafeRowPair.value, projection)
              recordCnt = recordCnt + 1
              currentDb.put(unsafeRowPair.key.getBytes, unsafeConverter(newValueRow).getBytes)
          }
        }
        val logDefaultValues = if (defaultValues.nonEmpty) s" using defaultValue $defaultValues" else ""
        logInfo( s"migrated schema for all values of $this took $tMigration sec for $recordCnt records $logDefaultValues")
      }
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
  override def doMaintenance(): Unit = synchronized {
    try {
      if (embeddedMaintenance) {
        logDebug(s"trigger embedded maintenance for $this")
        triggerMaintenance = true
      } else {
        execStateMaintenance()
      }
    } catch {
      case e: Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'doMaintenance' of $this")
        throw e
    }
  }

  private def execStateMaintenance(): Unit = {
    logDebug(s"starting state maintenance for $this")

    var deleteCnt = 0
    val t = MiscHelper.measureTime {

      val openedDb = if (currentDb == null) {
        currentDb = openDb
        true
      } else false

      // remove expired Records from state
      if (removeExpiredRowsInMaintenance) {
        val currentTime = System.currentTimeMillis()
        val groupStateValueColIndex = valueSchema.fieldIndex("groupState")
        val groupStateValueSchema = valueSchema(groupStateValueColIndex).dataType.asInstanceOf[StructType]
        if (!groupStateValueSchema.exists(_.name==removeExpiredRowsInMaintenanceColName)) {
          logWarning(s"""removeExpiredRowsInMaintenanceColName $removeExpiredRowsInMaintenanceColName not found in groupState cols ${groupStateValueSchema.map(_.name).mkString(", ")}""")
          removeExpiredRowsInMaintenance = false
        } else {
          val timeoutColIndex = groupStateValueSchema.fieldIndex(removeExpiredRowsInMaintenanceColName)
          val stateIterator = RocksDbHelper.getIterator(currentDb, keySchema, valueSchema)
          stateIterator.foreach {
            rowPair =>
              if (!rowPair.value.isNullAt(groupStateValueColIndex)) {
                val groupStateValueRow = rowPair.value.getStruct(groupStateValueColIndex, groupStateValueSchema.size)
                if (!groupStateValueRow.isNullAt(timeoutColIndex)) {
                  val timeout = groupStateValueRow.getLong(timeoutColIndex)
                  if (timeout > 0 && timeout <= currentTime) {
                    currentDb.delete(rowPair.key.getBytes)
                    deleteCnt = deleteCnt + 1
                  }
                }
              }
          }
        }
      }

      // flush WAL to SST Files
      currentDb.flush(new FlushOptions().setWaitForFlush(true))

      // do manual compaction if desired
      if (manualRocksDbCompaction) {
        currentDb.pauseBackgroundWork()
        currentDb.compactRange()
        currentDb.continueBackgroundWork()
      }
      if (openedDb) closeDb()

      // estimate db size
      rocksDbCurrentSize = FileUtils.sizeOfDirectory( new File(localDbDir))
      rocksDbBackupSize = localBackupFs.listStatus(localBackupSharedPath)
        .map(_.getLen).sum
    }
    val logDeleteCnt = if (removeExpiredRowsInMaintenance) s" deleted $deleteCnt keys," else ""
    logInfo(s"state maintenance for $this took $t secs,$logDeleteCnt current db files size: ${MiscHelper.formatBytes(rocksDbBackupSize)}, backup shared files size: ${MiscHelper.formatBytes(rocksDbCurrentSize)}")
  }

  private def closeDb(): Unit = {
    if (currentDb!=null) {
      currentDb.pauseBackgroundWork()
      currentDb.close()
      currentDb = null
    }
  }

  /**
    * Called when the provider instance is unloaded from the executor.
    */
  override def close(): Unit = synchronized {
    try {
      // cleanup
      closeDb()
      options.close()
      writeOptions.close()
      backupDBOptions.close()
      currentDbStats.close()
      FileUtils.deleteQuietly(new File(localDataDir))
      logInfo(s"Removed local db and backup dir of $this")
    } catch {
      case e:Exception =>
        logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'close' of $this")
    }
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
  private def createBackup(version: Long): Int = {

    val backupEngine = createBackupEngine

    // delete potential newer backups to avoid diverging data versions. See also comment for [[BackupEngine.restoreDbFromBackup]]
    val newerBackups = getBackupInfo(backupEngine).toSeq.filter{ case (v,b) => v > version}
    if (newerBackups.nonEmpty) {
      cleanupLocalBackups(backupEngine, newerBackups)
      cleanupRemoteBackupVersions(newerBackups.map(_._1))
      cleanupFutureRemoteSchemas(version)
      logInfo(s"deleted newer backup versions ${newerBackups.map(_._1).distinct.sorted.mkString(", ")} for $this")
    }

    // remember shared files before backup
    val sharedFilesBeforeBackup = localBackupFs.listStatus(localBackupSharedPath).map(_.getPath)

    // create local backup
    MiscHelper.verify(currentDb != null, "RocksDb must be opened")
    val tBackup = MiscHelper.measureTime {
      // Don't flush before backup, as also WAL is backuped. We can use WAL for efficient incremental backup
      backupEngine.createNewBackupWithMetadata(currentDb, version.toString, false)
    }
    val backupId = backupEngine.getBackupInfo.asScala.map(_.backupId()).max
    val sharedFilesCurrentBackup = RocksDbHelper.parseSharedFilesFromMetadata(localBackupDir, backupId).map( f => new Path(remoteBackupSharedPath,f))
    logDebug(s"created backup for $this version $version with backupId $backupId, took $tBackup secs")

    // cleanup old backups
    val t = cleanupOldLocalBackups(backupEngine)
    backupEngine.close

    // sync to remote filesystem
    val tSync = MiscHelper.measureTime {
      syncRemoteBackup(backupId, version, sharedFilesBeforeBackup, sharedFilesCurrentBackup)
    }
    logDebug(s"synced $this version $version backupId $backupId to remote filesystem, took $tSync secs")

    //return
    backupId
  }

  /**
    * copy local backup to remote filesystem
    */
  private def syncRemoteBackup(backupId: Int, version: Long, sharedFilesBeforeBackup: Seq[Path], sharedFilesCurrentBackup: Seq[Path]): String = {

    // diff & copy shared data files in parallel
    val sharedFiles2Copy = sharedFilesCurrentBackup.map(_.getName).diff(sharedFilesBeforeBackup.map(_.getName))
    sharedFiles2Copy.par.foreach( f =>
      copyLocalToRemoteFile(new Path(localBackupSharedPath,f), new Path(remoteBackupSharedPath,f), true)
    )
    logDebug(s"found ${sharedFiles2Copy.size} files to copy to remote filesystem for $this")

    // save metadata & private files as zip archive
    val metadataFile = new File(s"$localBackupDir${File.separator}meta${File.separator}$backupId")
    val privateFiles = new File(s"$localBackupDir${File.separator}private${File.separator}$backupId").listFiles().toSeq
    val tstamp = System.currentTimeMillis()
    val contextId = Option(TaskContext.get()).map( c => s"${c.stageId}.${c.stageAttemptNumber}.${c.taskAttemptId}.${c.attemptNumber()}").getOrElse("0")
    val remoteBackupFile = new Path(remoteBackupPath,s"${version}_${contextId}_$tstamp.zip")
    compressToRemoteFile(metadataFile +: privateFiles, remoteBackupFile)

    // save schema if changed
    ensureBackupSchemasUpToDate(version)

    // remember new remote backup
    remoteBackupInfos.put(version, RemoteBackupInfo(version, tstamp, remoteBackupFile, Seq(), Some(backupId), sharedFilesCurrentBackup))

    // delete old remote backups
    val remoteVersionsToDelete = remoteBackupInfos.keys.toSeq.sorted.dropRight(minRemoteVersionsToRetain)
    cleanupRemoteBackupVersions(remoteVersionsToDelete)
    cleanupRemoteBackupSharedFiles()

    //return
    version.toString
  }

  /**
    * keep latest x local backup versions, where x can be configured by minLocalBackupsToRetain
    */
  private def cleanupOldLocalBackups(backupEngine: BackupEngine): Seq[Long] = {
    val backupInfosToDelete = getBackupInfo(backupEngine).toSeq.sortBy(_._1).dropRight(minLocalBackupsToRetain)
    cleanupLocalBackups(backupEngine, backupInfosToDelete)
    logDebug( s"backup engine contains the following backups for $this: " + getBackupInfoStr(backupEngine))
    //return
    backupInfosToDelete.map(_._1)
  }

  private def cleanupLocalBackups(backupEngine: BackupEngine, backupInfos: Seq[(Long,Int)]): Unit = {
    // delete local backup versions
    backupInfos.map{ case (v,b) => b}.distinct.foreach( b => deleteLocalBackup( backupEngine, b))
    backupEngine.garbageCollect()
    // reset backupId in remoteBackupInfos
    backupInfos.map{ case (v,b) => v}.distinct.foreach { v =>
      remoteBackupInfos.get(v).foreach(backupInfo => remoteBackupInfos.put(v, backupInfo.copy(backupId = None)))
    }
  }

  private def cleanupRemoteBackupVersions(versions: Seq[Long]): Unit = {
    // note that we keep the shared files for now. They will be cleaned up later with the full sync of needed vs existing shared files
    if (cleanupRemoteBackups) {
      // delete remote version files
      versions.par.foreach { version =>
        remoteBackupInfos.get(version).foreach { backupInfo =>
          // delete backup file of this version including possibly existing earlier files for this version (altPath)
          remoteBackupFm.delete(backupInfo.path)
          backupInfo.altPath.foreach(remoteBackupFm.delete)
        }
      }
      // remove from remoteBackupInfos
      versions.foreach( remoteBackupInfos.remove )
    }
  }

  private def cleanupRemoteBackupSharedFiles(): Unit = {
    if (cleanupRemoteBackups) {
      // diff needed vs existing files
      // note that we only need listStatus on remote filesystem if cleanRemoteBackups is enabled (performance!)
      val sharedFilesNeeded = remoteBackupInfos.values.flatMap(_.sharedFiles.map(_.getName)).toSeq.distinct
      val sharedFilesExisting = remoteBackupFs.listStatus(remoteBackupSharedPath).map(_.getPath.getName).toSeq
      val sharedFilesToDelete = sharedFilesExisting.diff(sharedFilesNeeded)
      // delete files no longer needed
      sharedFilesToDelete.foreach( f => remoteBackupFm.delete( new Path(remoteBackupSharedPath, f)))
      logDebug(s"found ${sharedFilesToDelete.size} files to delete from remote filesystem for $this")
    }
  }

  private def cleanupFutureRemoteSchemas( currentVersion: Long ): Unit = {
    // delete future schema versions - to avoid later conflicts
    backupSchemas.filter(_._1 > currentVersion)
      .foreach{ case (v, t, s) =>
        remoteBackupFm.delete( new Path(remoteBackupSchemaPath, getRemoteBackupSchemaFilename(v, t)))
      }
  }

  /**
    * delete backup from backup engine
    */
  private def deleteLocalBackup(backupEngine: BackupEngine, backupId:Int): Unit = {
    Try(backupEngine.deleteBackup(backupId)) match {
      case Failure(e) => logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' while deleting backup with backupId $backupId for $this")
      case _ => Unit
    }
  }

  private def getHadoopFileBufferSize = hadoopConf.getInt("io.file.buffer.size", 4096)

  def copyLocalToRemoteFile( src: Path, dst: Path, overwriteIfPossible: Boolean ): Unit = {
    // For S3 there might be a FileNotFoundException on multi-part uploads which is not handled by retries from S3Client
    MiscHelper.retry( remoteUploadRetries, s"in method 'copyLocalToRemoteFile' while copying $src to $dst for $this", logWarning ) {
      org.apache.hadoop.io.IOUtils.copyBytes(localBackupFs.open(src), remoteBackupFm.createAtomic(dst, overwriteIfPossible), hadoopConf, true)
    }
  }

  def copyRemoteToLocalFile( src: Path, dst: Path, overwriteIfPossible: Boolean ): Unit = {
    org.apache.hadoop.io.IOUtils.copyBytes( remoteBackupFm.open(src), localBackupFs.create(dst, overwriteIfPossible), hadoopConf, true)
  }

  def compressToRemoteFile(localFiles: Seq[File], archiveFile: Path): Unit = {
    MiscHelper.retry( remoteUploadRetries, s"in method 'compressToRemoteFile' while compressing $localFiles to $archiveFile for $this", logWarning _ ) {
      MiscHelper.compress2Remote(localFiles, archiveFile, remoteBackupFm, getHadoopFileBufferSize, Some(localBackupDir))
    }
  }

  def writeToRemoteFile(remoteFile: Path, overwriteIfPossible: Boolean)(writerCode: DataOutputStream => Unit): Unit = {
    MiscHelper.retry( remoteUploadRetries, s"in method 'writeToRemoteFile' while writing to $remoteFile for $this", logWarning ) {
      val os = remoteBackupFm.createAtomic( remoteFile, overwriteIfPossible)
      writerCode(os)
      os.close()
    }
  }


  /**
  * Get name for local data directory.
  * As this might be on a persistent volume we include the hostname to avoid conflicts
  */
  private def getDataDirName(sparkJobName: Option[String]): String = {
    val sparkJobNamePrep = sparkJobName.filter(!_.isEmpty).map(_+"-").getOrElse("")
    // the state store name is in the current spark version empty (2.4.0)
    val stateStoreNamePrep = Some(stateStoreId_.storeName).filter(!_.isEmpty).map(_+"-").getOrElse("")
    // we need a random number to allow multiple streaming queries running with different state stores in the same spark job and executor.
    s"sparklocalstate-$sparkJobNamePrep${stateStoreId_.operatorId}-${stateStoreId_.partitionId}-$stateStoreNamePrep${MiscHelper.getHostName}-${MiscHelper.getRandomPositiveInt}"
  }

  /**
    * Get iterator of all the data of the latest version of the store.
    */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = {
    val maxVersion = remoteBackupInfos.keys.max
    getStore(maxVersion).iterator()
  }

  /*
   * parse backup schema file
   * if version is higher than given version, the corresponding file has to be ignored and deleted as it is from an uncomitted version...
   */
  private def parseBackupSchemaFile(latestVersion: Long)(path: Path): Option[(Long,String,StructType)] = Try {
    val Array(version, typ, _) = path.getName.split('.')
    if (version.toLong<=latestVersion) {
      val is = remoteBackupFm.open(path)
      val schemaStr = using(Source.fromInputStream(is))(_.mkString)
      val schema = DataType.fromJson(schemaStr).asInstanceOf[StructType]
      Some(version.toLong, typ, schema)
    } else {
      remoteBackupFm.delete(path)
      None
    }
  } match {
    case Success(x) => x
    case Failure(e) =>
      logError(s"Could not read/parse schema $path. Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'parseBackupSchemaFile' of $this ")
      throw e
  }

  /*
   * lookup last backup before given version for given type key/value
   */
  private def getBackupSchema(version: Long, typ: String): Option[StructType] = {
    backupSchemas.filter( s => s._1<=version && s._2==typ).sortBy(_._1).map(_._3).lastOption
  }
  def getBackupKeySchema(version: Long): Option[StructType] = getBackupSchema(version, "key")
  def getBackupValueSchema(version: Long): Option[StructType] = getBackupSchema(version, "value")

  /*
   * list remote backup schema files
   */
  def getRemoteBackupSchemaFiles: Seq[Path] = try {
    remoteBackupFm.list(remoteBackupSchemaPath, new PathFilter {
      override def accept(path: Path): Boolean = path.getName.endsWith(".schema")
    }).map(_.getPath).toSeq
  } catch {
    case _:FileNotFoundException =>
      logInfo(s"remote backup schema path $remoteBackupSchemaPath is not existing")
      Seq()
  }

  /*
   * update backup schemas if needed
   */
  def ensureBackupSchemasUpToDate(version:Long) = {
    if (!backupKeySchemaUpToDate) {
      val backupKeySchema = getBackupKeySchema(version)
      if (backupKeySchema.isEmpty || (keySchema != null && backupKeySchema.get != keySchema)) {
        writeRemoteBackupSchema(version, "key", keySchema)
      }
      backupKeySchemaUpToDate = true
    }
    if (!backupValueSchemaUpToDate) {
      val backupValueSchema = getBackupValueSchema(version)
      if (backupValueSchema.isEmpty || (valueSchema != null && backupValueSchema.get != valueSchema)) {
        writeRemoteBackupSchema(version, "value", valueSchema)
      }
      backupValueSchemaUpToDate = true
    }
  }

  /*
   * write remote backup schema file
   */
  def writeRemoteBackupSchema(version: Long, typ: String, schema: StructType): Unit = {
    writeToRemoteFile( new Path(remoteBackupSchemaPath, getRemoteBackupSchemaFilename(version,typ)), false){ os =>
      os.writeBytes(schema.prettyJson)
    }
    logInfo(s"created new backup $typ schema for version $version: ${schema.json}")
  }

  /**
    * get filename for remote backup schema file
    */
  def getRemoteBackupSchemaFilename(version: Long, typ: String) = s"$version.$typ.schema"
}

/**
  * Companion object with constants.
  */
object RocksDbStateStoreProvider extends Logging {
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

  final val DEFAULT_STATE_EXPIRY_STRICT_MODE: String = "false"

  final val STATE_LOCAL_DIR: String = "spark.sql.streaming.stateStore.localDir"

  final val DEFAULT_STATE_LOCAL_DIR: String = Option(System.getenv("SPARK_LOCAL_DIRS")).map(_.split(',').head).getOrElse(getJavaTempDir)

  final val STATE_LOCAL_WAL_DIR: String = "spark.sql.streaming.stateStore.localWalDir"

  final val DEFAULT_STATE_LOCAL_WAL_DIR: String = getJavaTempDir

  final val STATE_COMPRESSION_TYPE: String = "spark.sql.streaming.stateStore.compressionType"

  final val DEFAULT_STATE_COMPRESSION_TYPE: String = null // NO_COMPRESSION

  final val STATE_CLOSE_DB_AFTER_COMMIT: String = "spark.sql.streaming.stateStore.closeDbAfterCommit"

  final val DEFAULT_STATE_CLOSE_DB_AFTER_COMMIT: String = "true"

  final val STATE_MANUAL_ROCKS_DB_COMPACTION: String = "spark.sql.streaming.stateStore.manualRocksDbCompaction"

  final val DEFAULT_STATE_MANUAL_ROCKS_DB_COMPACTION: String = "false"

  final val STATE_EMBEDDED_MAINTENANCE: String = "spark.sql.streaming.stateStore.embeddedMaintenance"

  final val DEFAULT_STATE_EMBEDDED_MAINTENANCE: String = "false"

  final val REMOTE_UPLOAD_RETRIES: String = "spark.sql.streaming.stateStore.remoteUpload.retries"

  final val DEFAULT_REMOTE_UPLOAD_RETRIES: String = "1"

  final val STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE: String = "spark.sql.streaming.stateStore.removeExpiredRowsInMaintenance"

  final val DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE: String = "false"

  final val STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME: String = "spark.sql.streaming.stateStore.removeExpiredRowsInMaintenanceColName"

  final val DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME: String = "expirationTstmp"

  final val STATE_SCHEMAEVOLUTION_DEFAULTVALUE_PREFIX: String = "spark.sql.streaming.stateStore.schemaEvolution.defaultValue"

  final val DEFAULT_STATE_CLEANUP_REMOTE_BACKUPS: String = "true"

  final val STATE_CLEANUP_REMOTE_BACKUPS: String = "spark.sql.streaming.stateStore.cleanupRemoteBackups"

  final val STATE_MIN_LOCAL_BACKUPS_TO_RETAIN: String = "spark.sql.streaming.stateStore.minLocalBackupsToRetain"

  final val DEFAULT_STATE_LOAD_REMOTE_BACKUP_SELECTIVE: String = "true"

  final val STATE_LOAD_REMOTE_BACKUP_SELECTIVE: String = "spark.sql.streaming.stateStore.loadRemoteBackupSelective"

  final val DUMMY_VALUE: String = ""

  private def getJavaTempDir = System.getProperty("java.io.tmpdir").replace('\\', '/')

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

  /**
    * Creates a mapping of Streaming Query and its expiry timeout (seconds).
    * For backward compatibility, an additional entry is done for [[UNNAMED_QUERY]]'s
    *
    * The timeout value for [[UNNAMED_QUERY]]'s is set by the value of [[STATE_EXPIRY_SECS]]
    *
    * @param stateStoreConf state store config map set on [[SparkConf]]
    * @return mapping of queryName -> expirySecs
    */

  private def getExpirationByQuery(stateStoreConf: Map[String, String]): Map[String, Int] =
    stateStoreConf
      .filterKeys(_.startsWith(s"$STATE_EXPIRY_SECS."))
      .map { case (key, value) => key.replace(s"$STATE_EXPIRY_SECS.", "") -> getTTL(value) }
      .+(UNNAMED_QUERY -> stateStoreConf.getOrElse(STATE_EXPIRY_SECS, DEFAULT_STATE_EXPIRY_SECS).toInt)

  /**
    * Helper method to check if the given string is an integer
    *
    * @param value [[String]] value to be checked
    * @return [[Option]] if value is not an integer returns [[None]] else [[Some]]
    */
  private def toInt(value: String): Option[Int] = {
    Try(value.toInt) match {
      case Success(v) => Some(v)
      case Failure(_) => None
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
  }

  private def getTTL(expirySecs: String): Int = toInt(expirySecs) match {
    case Some(value) => value
    case None =>
      throw new IllegalArgumentException(
        s"Provided value '$expirySecs' is invalid. Expiry Secs must be an Integer."
      )
  }

  private def setExpireMode(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_EXPIRY_STRICT_MODE, DEFAULT_STATE_EXPIRY_STRICT_MODE).toBoolean) match {
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

  private def setManualRocksDbCompaction(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_MANUAL_ROCKS_DB_COMPACTION, DEFAULT_STATE_MANUAL_ROCKS_DB_COMPACTION).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setEmbeddedMaintenance(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_EMBEDDED_MAINTENANCE, DEFAULT_STATE_EMBEDDED_MAINTENANCE).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setRemoteUploadRetries(conf: Map[String, String]): Int =
    Try(conf.getOrElse(REMOTE_UPLOAD_RETRIES, DEFAULT_REMOTE_UPLOAD_RETRIES).toInt) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setRemoveExpiredRowsInMaintenance(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE, DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setRemoveExpiredRowsInMaintenanceColName(conf: Map[String, String]): String =
    Try(conf.getOrElse(STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME, DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setCleanupRemoteBackups(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_CLEANUP_REMOTE_BACKUPS, DEFAULT_STATE_CLEANUP_REMOTE_BACKUPS).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setMinLocalBackupsToRetain(conf: Map[String, String], default: Int): Int =
    Try(conf.get(STATE_MIN_LOCAL_BACKUPS_TO_RETAIN).map(_.toInt).getOrElse(default)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  private def setLoadRemoteBackupSelective(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(STATE_LOAD_REMOTE_BACKUP_SELECTIVE, DEFAULT_STATE_LOAD_REMOTE_BACKUP_SELECTIVE).toBoolean) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }

  /**
    * we share a global block cache to avoid memory leaks because block cache is not cleaned up properly on db.close
    */
  private var globalBlockCache: Cache = null
  def getGlobalBlockCache(size: Long): Cache = synchronized {
    if (globalBlockCache==null) {
      globalBlockCache = new LRUCache(size)
      logInfo(s"initialize global block cache with size ${MiscHelper.formatBytes(size)}")
    }
    globalBlockCache
  }
}

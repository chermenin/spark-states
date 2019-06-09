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
import java.util.zip.ZipInputStream

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SchemaHelper
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.{DataType, StructType}
import org.rocksdb.{TickerType, _}
import org.rocksdb.util.SizeUnit

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
      * Commit all the updates that have been made to the store, and return the new version.
      */
    override def commit(): Long = RocksDbStateStoreProvider.this.synchronized  {
      try {
        MiscHelper.verify(state == State.Updating, "Cannot commit already committed or aborted state")

        updateStatistics()

        state = State.Committed
        createBackup(newVersion)
        if (closeDbOnCommit) {
          currentDb.close()
          currentDb = null
        }

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
        if (closeDbOnCommit && currentDb!=null) {
          currentDb.close()
          currentDb = null
        }

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
      MiscHelper.verify(currentDb != null, "iterator can only be created if RocksDB is still opened")

      val stateFromRocksIter = RocksDbHelper.getIterator(currentDb, keySchema, valueSchema)
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
    override def metrics: StateStoreMetrics = {
      val estimatedKeyNb: Long = rocksdbVolumeStatistics.getOrElse(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY, 0)
      val memTableSize: Long = rocksdbVolumeStatistics.getOrElse(ROCKSDB_SIZE_ALL_MEM_TABLES, 0)
      StateStoreMetrics( estimatedKeyNb, memTableSize + rocksDbSharedFilesSize, Map.empty)
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
  @volatile private var rocksDbSharedFilesSize: Long = 0
  @volatile private var remoteUploadRetries: Int = 1
  @volatile private var removeExpiredRowsInMaintenance: Boolean = _
  @volatile private var removeExpiredRowsInMaintenanceColName: String = _
  private var remoteBackupPath: Path = _
  private var remoteBackupFs: FileSystem = _
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
  private var remoteBackupSchemaPath: Path = _
  private var backupSchemas: Seq[(Long,String,StructType)] = Seq()
  private var backupKeySchemaUpToDate = false
  private var backupValueSchemaUpToDate = false

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
      this.rotatingBackupKey = setRotatingBackupKey(storeConf.confs)
      this.closeDbOnCommit = setCloseDbAfterCommit(storeConf.confs)
      this.remoteUploadRetries = setRemoteUploadRetries(storeConf.confs)
      this.removeExpiredRowsInMaintenance = setRemoveExpiredRowsInMaintenance(storeConf.confs)
      this.removeExpiredRowsInMaintenanceColName = setRemoveExpiredRowsInMaintenanceColName(storeConf.confs)

      // initialize paths
      remoteBackupPath = stateStoreId.storeCheckpointLocation()
      remoteBackupFs = remoteBackupPath.getFileSystem(hadoopConf)
      localBackupDir = MiscHelper.createLocalDir(localDataDir+"/backup")
      localBackupPath = new Path("file:///"+localBackupDir)
      localBackupFs = localBackupPath.getFileSystem(hadoopConf)
      localDbDir = MiscHelper.createLocalDir(localDataDir+"/db")
      remoteBackupFm = CheckpointFileManager.create(remoteBackupPath, hadoopConf)
      remoteBackupSchemaPath = new Path(remoteBackupPath, "schema")

      // initialize empty database
      currentDbStats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS)
      options = new Options()
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
        .setDisableAutoCompactions(true) // we trigger manual compaction during state maintenance with compactRange()
        .setWalDir(localWalDataDir) // Write-ahead-log should be saved on fast disk (local, not NAS...)
        .setCompressionType(setCompressionType(storeConf.confs))
        .setCompactionStyle(CompactionStyle.UNIVERSAL)
      writeOptions = new WriteOptions()
        .setDisableWAL(false) // we use write ahead log for efficient incremental state versioning
      backupDBOptions = new BackupableDBOptions(localBackupDir.toString)
        .setShareTableFiles(true)
        .setShareFilesWithChecksum(setShareRocksDbFilesWithChecksum(storeConf.confs))
        .setSync(true)

      // restore backups from remote to local directory
      restoreFromRemoteBackups()

      // read and check state schemas
      val backupSchemaFiles = getRemoteBackupSchemaFiles
      val latestVersion = Try(backupList.keys.max).getOrElse(-1l)
      backupSchemas = backupSchemaFiles.flatMap( parseBackupSchemaFile(latestVersion))
      val latestBackupKeySchema = getBackupKeySchema(latestVersion)
      val latestBackupValueSchema = getBackupValueSchema(latestVersion)
      if (latestBackupKeySchema.isEmpty) logWarning(s"latest backup key schema not found (version=$latestVersion)")
      else if (keySchema!=null && latestBackupKeySchema.get!=keySchema) logWarning(s"latest backup key schema is different from current schema: latestBackup=${latestBackupKeySchema.get.json}, current=${keySchema.json}")
      if (latestBackupValueSchema.isEmpty) logWarning(s"latest backup value schema not found (version=$latestVersion)")
      else if (valueSchema!=null && latestBackupValueSchema.get!=valueSchema) logWarning(s"latest backup value schema is different from current schema: latestBackup=${latestBackupValueSchema.get.json}, current=${valueSchema.json}")

      logInfo(s"initialized $this, localDataDir=$localDataDir, localWalDataDir=$localWalDataDir")
    } catch {
      case e:Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'init' of $this")
        throw e
    }
  }

  protected def restoreFromRemoteBackups(): Unit = {
    // copy backups from remote storage and init backup engine
    backupList.clear
    if (remoteBackupFm.exists(remoteBackupPath)) {
      logDebug(s"loading state backup from remote filesystem $remoteBackupPath for $this")

      // cleanup possible existing backup files
      // Note: deleting localBackupDir on Windows results in permission errors in unit tests... we do the cleanup only for linux for now
      if (!MiscHelper.isWindowsOS) {
        FileUtils.deleteQuietly(new File(localBackupDir))
      }

      // read index file into backupList, otherwise extract backup information from archive files
      val backupList = Try(remoteBackupFm.open(new Path(remoteBackupPath, "index"))) match {
        case Success(backupListInputStream) =>
          val backupListReader = new BufferedReader(new InputStreamReader(backupListInputStream))
          val backupListParsed = backupListReader.lines.iterator.asScala.toArray.map(_.split(',').map(_.trim.split(':')).map(e => (e(0).trim, e(1).trim)).toMap)
          backupListInputStream.close() // it is tricky that input stream isn't closed at all or even twice... iterator.toArray / manual close seems to be proper
          backupListParsed.map(b => b("version").toLong -> (b("backupId").toInt, b("backupKey"))).toMap
        case Failure(_: FileNotFoundException) =>
          logWarning(s"index file not found on remote filesystem for $this. Extracted the backup list from archive files.")
          getBackupListFromRemoteFiles
        case Failure(e) => throw e
      }
      logDebug(s"backup list for $this: " + backupList.toSeq.sortBy(_._1).map(b => s"v=${b._1},b=${b._2._1},k=${b._2._2}").mkString("; "))

      // load files
      val t = MiscHelper.measureTime {

        // copy shared files in parallel
        remoteBackupFm.list(new Path(remoteBackupPath, "shared")).toSeq
          .par.foreach(f => try {
            copyRemoteToLocalFile(f.getPath, new Path(localBackupPath, s"shared/${f.getPath.getName}"), false)
          } catch {
            // This is to be tolerant in case of inconsistent S3 metadata: the object might be deleted but it's still listed in state
            // In this case we can catch the FileNotFoundException, log it as error and do not fail the job.
            // This is no problem for state consistency, as the problematic file should have been deleted. It's no longer needed.
            case e:FileNotFoundException => logError( s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'restoreFromRemoteBackups' while reading shared remote backup files. This is an inconsistency between S3 object and metadata. Please cleanup manually the no longer existing file.")
          })
        // copy metadata/private files according to backupList in parallel
        backupList.values.map { case (backupId, backupKey) => s"$backupKey.zip" }.par
          .foreach(filename => try {
            MiscHelper.decompressFromRemote(new Path(remoteBackupPath, filename), localBackupDir, remoteBackupFm, getHadoopFileBufferSize)
          } catch {
            case e: FileNotFoundException => logWarning(s"{e.getMessage} when copying metadata/private files from remote backup in method 'init'")
          })
        backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)

        // cleanup potential superfluous backups in backup engine compared to index/backuplist
        if (backupList.size > 0) {
          backupEngine.getBackupInfo.asScala
            .filter(_.appMetadata().toLong > backupList.keys.max)
            .foreach { backupInfo =>
              deleteBackup(backupInfo.backupId)
              logInfo(s"deleted superfluous backup for version ${backupInfo.appMetadata}, backupId ${backupInfo.backupId} from backup engine for $this")
            }
        }
        cleanupOldBackups()

        // check backupIds between backup engine and index/backuplist
        backupEngine.getBackupInfo.asScala
          .foreach { backupEngineEntry =>
            val version = backupEngineEntry.appMetadata.toLong
            val backupListEntry = backupList.get(version)
            if (backupListEntry.isEmpty) logWarning(s"missing backup in index/backuplist for version $version of $this")
            else if (backupListEntry.get._1 != backupEngineEntry.backupId) logWarning(s"conflicting backupId's for version $version of $this. index/backuplist has backupId ${backupListEntry.get._1}, backupEngine has backupId ${backupEngineEntry.backupId}")
          }
      }
      this.backupList ++= backupList // copy local backup list variable to state variable if everything went smooth.
      logInfo(s"got state backup from remote filesystem $remoteBackupPath for $this, took $t secs")

    } else {
      logDebug(s"initializing state backup at $remoteBackupPath for $this")
      remoteBackupFm.mkdirs( new Path(remoteBackupPath, "shared"))
      backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)
      currentDb = openDb
      createBackup(0L)
      currentDb.close()
      currentDb = null
    }
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

    // try restoring remote backups if version doesnt exists in local backups. This might happen if execution of a partition changes executor.
    if (!backupList.contains(version)) {
      restoreFromRemoteBackups()
      if (!backupList.contains(version)) throw new IllegalStateException(s"Can not find version $version in backup list (${backupList.keys.toSeq.sorted.mkString(",")})")
    }
    val t = MiscHelper.measureTime {
      restoreAndOpenDb(version)
      // when exporting state, key/valueSchema is set to null and we need to use the backup key/value schemas
      currentStore = new RocksDbStateStore(version
        , Option(keySchema).orElse(getBackupKeySchema(version)).get
        , Option(valueSchema).orElse(getBackupValueSchema(version)).get, cache)
    }
    logInfo(s"Retrieved $currentStore for $this version $version, took $t seconds")
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

      // get infos about backup to recover
      val backupIdRecovery = backupList.get(version).map{ case (b,k) => b }
        .getOrElse(throw new IllegalStateException(s"backup for version $version not found"))

      // close db and restore backup
      val tRestore = MiscHelper.measureTime {
        if (currentDb != null) {
          currentDb.close() // we need to close before restore
          currentDb = null
        }
        val restoreOptions = new RestoreOptions(false)
        backupEngine.restoreDbFromBackup(backupIdRecovery, localDbDir, localWalDataDir, restoreOptions)
        restoreOptions.close()
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
        val projection = SchemaHelper.getSchemaProjection(backupValueSchema.get, valueSchema)
        logInfo( s"value schema evolution needed for $this. Projection: $projection" )
        // migrate all records
        val tMigration = MiscHelper.measureTime {
          val allKVIter = RocksDbHelper.getIterator(currentDb, backupKeySchema.get, backupValueSchema.get)
          val unsafeConverter = UnsafeProjection.create(valueSchema) // InternalRow -> UnsafeRow
          allKVIter.foreach {
            unsafeRowPair =>
              val newValueRow = SchemaHelper.applySchemaProjection(unsafeRowPair.value, projection)
              currentDb.put(unsafeRowPair.key.getBytes, unsafeConverter(newValueRow).getBytes)
          }
        }
        logInfo( s"migrated schema for all values of $this, took $tMigration sec")
      }

      // delete potential newer backups to avoid diverging data versions. See also comment for [[BackupEngine.restoreDbFromBackup]]
      val newerBackups = (backupList.toSeq.map{ case (v,(b,k)) => (v,b)} ++
          backupEngine.getBackupInfo.asScala.map( i => (i.appMetadata().toLong,i.backupId))
        ).filter{ case (v,b) => v > version}
      val tCleanup = MiscHelper.measureTime {
        remoteCleanupList.clear
        newerBackups.map{ case (v,b) => b}.distinct.foreach( deleteBackup )
        newerBackups.map{ case (v,b) => v}.distinct.foreach { v =>
          backupList.remove(v)
          remoteCleanupList += new Path(remoteBackupPath, s"${getBackupKey(v)}.zip")
        }

        backupEngine.garbageCollect()

        // delete unused shared data files to avoid later conflicts
        val (_,sharedFiles2Del) = getRemoteSyncList(new Path(remoteBackupPath,"shared"), new Path(localBackupPath,"shared"), _ => true, true )
        if(sharedFiles2Del.nonEmpty) logInfo(s"found ${sharedFiles2Del.size} unused remote files to delete for $this")
        remoteCleanupList ++= sharedFiles2Del

        // delete future schema versions to avoid later conflicts
        backupSchemas.filter(_._1>version).foreach{ case (v,t,_) =>
          val path = new Path(remoteBackupSchemaPath, getRemoteBackupSchemaFilename(v,t))
          logInfo(s"found future schema version to delete for $this")
          remoteCleanupList += path
        }
      }
      if (newerBackups.nonEmpty) logInfo(s"deleted newer local backups versions ${newerBackups.map(_._1).distinct.sorted.mkString(", ")} for $this, took $tCleanup secs")
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
      logDebug(s"starting doMaintenance for $this")

      val t = MiscHelper.measureTime {

        // get Db
        val opened = if (currentDb == null) {
          currentDb = openDb
          true
        } else false

        // remove expired Records from state
        if (removeExpiredRowsInMaintenance) {
          val currentTime = System.currentTimeMillis()
          val groupStateValueColIndex = valueSchema.fieldIndex("groupState")
          val groupStateValueSchema = valueSchema(groupStateValueColIndex).dataType.asInstanceOf[StructType]
          val timeoutColIndex = groupStateValueSchema.fieldIndex(removeExpiredRowsInMaintenanceColName)
          val stateIterator = RocksDbHelper.getIterator(currentDb, keySchema, valueSchema)
          stateIterator.foreach {
            rowPair =>
              if (!rowPair.value.isNullAt(groupStateValueColIndex)) {
                val groupStateValueRow = rowPair.value.getStruct(groupStateValueColIndex, groupStateValueSchema.size)
                if (!groupStateValueRow.isNullAt(timeoutColIndex)) {
                  val timeout = groupStateValueRow.getLong(timeoutColIndex)
                  if (timeout>0 && timeout <= currentTime) currentDb.delete(rowPair.key.getBytes)
                }
              }
          }
        }

        // flush WAL to SST Files and do manual compaction
        currentDb.flush(new FlushOptions().setWaitForFlush(true))
        currentDb.pauseBackgroundWork()
        currentDb.compactRange()
        currentDb.continueBackgroundWork()
        if (opened) {
          currentDb.close()
          currentDb = null
        }

        // cleanup old backups
        cleanupOldBackups()

        // estimate db size
        rocksDbSharedFilesSize = localBackupFs.listStatus(new Path(localBackupPath, "shared"))
          .map(_.getLen).sum

        // remove cleaned up backups from backup list
        val backupInfoVersions = backupEngine.getBackupInfo.asScala.map(_.appMetadata().toLong)
        val removedBackups = backupList.keys.filter(v => !backupInfoVersions.contains(v))
        removedBackups.foreach(backupList.remove)
        logDebug(s"backup list for $this after doMaintenance: ${backupList.toSeq.sortBy(_._1).mkString(", ")}")

        // check free diskspace
        //val dirFreeSpace = Seq(localDbDir, localWalDataDir).distinct.map( f => s"$f=${new File(f).getUsableSpace().toFloat/1024/1024}MB")
        //logInfo(s"free disk space for $this: "+dirFreeSpace.mkString(", "))
      }
      logInfo(s"doMaintenance for $this took $t secs, shared file size is ${MiscHelper.formatBytes(rocksDbSharedFilesSize)}")
    } catch {
      case e: Exception =>
        logError(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'doMaintenance' of $this")
        throw e
    }
  }

  /**
    * Called when the provider instance is unloaded from the executor.
    */
  override def close(): Unit = synchronized {
    try {
      // cleanup
      if(currentDb!=null) {
        currentDb.close()
        currentDb = null
      }
      backupEngine.close()
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

    // save schema if changed
    ensureBackupSchemasUpToDate(version)

    // update index of backups
    backupList.find{ case (v,(b,k)) => k == backupKey}
      .foreach{ case (v,(b,k)) => backupList.remove(v)} // remove existing entry for this backup key
    backupList.put(version, (backupId,backupKey))
    syncRemoteIndex()

    // delete old data files in parallel
    sharedFiles2Del.par.foreach( f => remoteBackupFm.delete(f))

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
    backupsToDelete.foreach( b => deleteBackup(b.backupId()))
    backupEngine.garbageCollect()
    logDebug( s"backup engine contains the following backups for $this: " + backupEngine.getBackupInfo.asScala.map(b => s"v=${b.appMetadata()},b=${b.backupId}").mkString("; "))
  }

  /**
    * delete backup from backup engine
    */
  private def deleteBackup(backupId:Int): Unit = {
    Try(backupEngine.deleteBackup(backupId)) match {
      case Failure(e) => logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' while deleting backup with backupId $backupId for $this")
      case _ => Unit
    }
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
    val remoteFiles = try {
      remoteBackupFm.list(remotePath, new PathFilter {
        override def accept(path: Path) = nameFilter(path.getName)
      }).toSeq
    } catch {
      // ignore if remote directory doesn't exist. We will (re-)create it during synchronization of files.
      case e:FileNotFoundException =>
        logWarning(s"'${e.getClass.getSimpleName}: ${e.getMessage}' in getRemoteSyncList while getting remote files. Going on with empty remote file list.")
        Seq()
    }
    val localFiles = localBackupFs.listStatus(localPath, new PathFilter {
      override def accept(path: Path) = nameFilter(path.getName)
    }).toSeq

    // find local files not existing or different in remote dir
    // exception: if this task is a retry, we will copy all local files to avoid later checksum conflicts (otherwise we would need to calculate a checksum and compare between remote & local file)
    val isTaskRetry = Option(TaskContext.get()).map(_.attemptNumber()>0).getOrElse(false)
    val files2Copy = if (isTaskRetry) {
      logInfo("this task is a retry, copying all local files to remote to avoid later rocksdb checksum conflicts")
      localFiles.map(_.getPath)
    } else {
      localFiles.flatMap( lf =>
        remoteFiles.find( rf => rf.getPath.getName==lf.getPath.getName ) match {
          case None => Some(lf) // not existing -> copy
          case Some(rf) if rf.getLen!=lf.getLen => // existing and different -> copy
            if (errorOnChange) logError(s"Remote file ${rf.getPath} is different from local file ${lf.getPath}. Overwriting for now.")
            Some(lf)
          case _ => None // existing and the same -> nothing
        }
      ).map(_.getPath)
    }

    // delete remote files not existing in local dir
    val files2Del = remoteFiles.filter( rf => !localFiles.exists( lf => lf.getPath.getName==rf.getPath.getName ))
      .map(_.getPath)

    // avoid all remote files being deleted
    if (files2Copy.isEmpty && remoteFiles.nonEmpty && files2Del.size==remoteFiles.size) {
      logWarning(s"Synchronization of local -> remote would delete all remote files. This is strange and is avoided for now. Local files: ${localFiles.mkString(", ")}. Remote files: ${remoteFiles.mkString(", ")}.")
      (files2Copy, Seq())
    } else (files2Copy,files2Del)
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

  def copyLocalToRemoteFile( src: Path, dst: Path, overwriteIfPossible: Boolean, retryCnt:Int = 0 ): Unit = try {
    org.apache.hadoop.io.IOUtils.copyBytes( localBackupFs.open(src), remoteBackupFm.createAtomic(dst, overwriteIfPossible), hadoopConf, true)
  } catch {
    // For S3 there might be a FileNotFoundException on multi-part uploads which is not handled by retries from S3Client
    case e:FileNotFoundException if retryCnt<remoteUploadRetries =>
      logWarning(s"Error '${e.getClass.getSimpleName}: ${e.getMessage}' in method 'copyLocalToRemoteFile' while copying $src to $dst for $this")
      copyLocalToRemoteFile( src, dst, overwriteIfPossible, retryCnt+1)
  }

  def copyRemoteToLocalFile( src: Path, dst: Path, overwriteIfPossible: Boolean ): Unit = {
    org.apache.hadoop.io.IOUtils.copyBytes( remoteBackupFm.open(src), localBackupFs.create(dst, overwriteIfPossible), hadoopConf, true)
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
    val maxVersion = backupList.keys.max
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
      val schemaStr = Source.fromInputStream(is).mkString
      is.close()
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
    val os = remoteBackupFm.createAtomic( new Path(remoteBackupSchemaPath, getRemoteBackupSchemaFilename(version,typ)), false)
    os.writeBytes(schema.prettyJson)
    os.close()
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

  final val DEFAULT_STATE_EXPIRY_METHOD: String = "false"

  final val STATE_LOCAL_DIR: String = "spark.sql.streaming.stateStore.localDir"

  final val DEFAULT_STATE_LOCAL_DIR: String = Option(System.getenv("SPARK_LOCAL_DIRS")).map(_.split(',').head).getOrElse(getJavaTempDir)

  final val STATE_LOCAL_WAL_DIR: String = "spark.sql.streaming.stateStore.localWalDir"

  final val DEFAULT_STATE_LOCAL_WAL_DIR: String = getJavaTempDir

  final val STATE_ROTATING_BACKUP_KEYS: String = "spark.sql.streaming.stateStore.rotatingBackupKeys"

  final val DEFAULT_STATE_ROTATING_BACKUP_KEYS: String = "false"

  final val STATE_COMPRESSION_TYPE: String = "spark.sql.streaming.stateStore.compressionType"

  final val DEFAULT_STATE_COMPRESSION_TYPE: String = null // NO_COMPRESSION

  final val STATE_CLOSE_DB_AFTER_COMMIT: String = "spark.sql.streaming.stateStore.closeDbAfterCommit"

  final val DEFAULT_STATE_CLOSE_DB_AFTER_COMMIT: String = "true"

  final val SHARE_ROCKSDB_FILES_WITH_CHECKSUM: String = "spark.sql.streaming.stateStore.shareRocksDbFilesWithChecksum"

  final val DEFAULT_SHARE_ROCKSDB_FILES_WITH_CHECKSUM: String = "false"

  final val REMOTE_UPLOAD_RETRIES: String = "spark.sql.streaming.stateStore.remoteUpload.retries"

  final val DEFAULT_REMOTE_UPLOAD_RETRIES: String = "1"

  final val STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE: String = "spark.sql.streaming.stateStore.removeExpiredRowsInMaintenance"

  final val DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE: String = "false"

  final val STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME: String = "spark.sql.streaming.stateStore.removeExpiredRowsInMaintenanceColName"

  final val DEFAULT_STATE_REMOVE_EXPIRED_ROWS_IN_MAINTENANCE_COL_NAME: String = "expirationTstmp"

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

  private def setShareRocksDbFilesWithChecksum(conf: Map[String, String]): Boolean =
    Try(conf.getOrElse(SHARE_ROCKSDB_FILES_WITH_CHECKSUM, DEFAULT_SHARE_ROCKSDB_FILES_WITH_CHECKSUM).toBoolean) match {
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

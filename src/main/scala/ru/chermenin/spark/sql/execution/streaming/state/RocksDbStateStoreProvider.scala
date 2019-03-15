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
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path, PathFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

/**
  * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
  * by files in a HDFS-compatible file system using RocksDB key-value storage format.
  * All updates to the store has to be done in sets transactionally, and each set of updates
  * increments the store's version. These versions can be used to re-execute the updates
  * (by retries in RDD operations) on the correct version of the store, and regenerate
  * the store version.
  * RocksDB Files will be kept in local filesystem and state store versions are uploaded to remote
  * filesystem on commit. To do efficient versioning, sRocksDB backup functionality can be used which
  * saves incremental backups to another location in the local filesystem.
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
  *
  *  TODO: It should be possible to configure WAL-Files in a separate (faster) location...
  *  TODO: implement rotatingBackupKey
  */
class RocksDbStateStoreProvider extends StateStoreProvider with Logging {

  /** Load native RocksDb library */
  RocksDB.loadLibrary()

  private val options: Options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(RocksDbStateStoreProvider.DEFAULT_WRITE_BUFFER_SIZE_MB * SizeUnit.MB)
    .setMaxWriteBufferNumber(RocksDbStateStoreProvider.DEFAULT_WRITE_BUFFER_NUMBER)
    .setDisableAutoCompactions(true) // we trigger manual compaction during state maintenance with compactRange()
    .setCompressionType(CompressionType.NO_COMPRESSION) // RocksDBException: Compression type Snappy is not linked with the binary (Windows)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  private val writeOptions = new WriteOptions()
    .setDisableWAL(false) // we use write ahead log for efficient incremental state versioning

  /** Implementation of [[StateStore]] API which is backed by RocksDB */
  class RocksDbStateStore(val version: Long,
                          val keySchema: StructType,
                          val valueSchema: StructType,
                          val store: RocksDB,
                          val keyCache: MapType) extends StateStore {

    /** New state version */
    private val newVersion = version + 1

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
        store.put(writeOptions, keyCopy.getBytes, valueCopy.getBytes)

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

        createBackup(newVersion)

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
        //TODO: how can we rollback uncommitted changes -> we should use a transaction!
        state = State.Aborted
        keysNumber =
          if (isStrictExpire) keyCache.size
          else store.getLongProperty(ROCKSDB_ESTIMATE_KEYS_NUMBER_PROPERTY)

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
      StateStoreMetrics(keysNumber, keysNumber * (keySchema.defaultSize + valueSchema.defaultSize), Map.empty)

    /**
      * Whether all updates have been committed
      */
    override def hasCommitted: Boolean = state == State.Committed

    /**
      * Custom toString implementation for this state store class.
      */
    override def toString: String =
      s"RocksDbStateStore[id=(op=${id.operatorId},part=${id.partitionId}),remoteDir=$remoteBackupPath]"

    def getCommittedVersion: Option[Long] = if(hasCommitted) Some(newVersion) else None

    private def getValue(key: UnsafeRow): UnsafeRow = {
      val valueBytes = store.get(key.getBytes)
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
  @volatile private var ttlSec: Int = _
  @volatile private var isStrictExpire: Boolean = _

  private var remoteBackupPath: Path = _
  private var remoteBackupFs: FileSystem = _
  private var localBackupPath: Path = _
  private var localBackupFs: FileSystem = _
  private var localBackupDir: String = _
  private var localDbDir: String = _
  private var currentDb: RocksDB = _
  private var currentStore: RocksDbStateStore = _
  private var backupEngine: BackupEngine = _

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
    this.localDataDir = createLocalDir( setLocalDir(storeConf.confs)+"/"+getDataDirName(hadoopConf.get("spark.app.name")))
    this.ttlSec = setTTL(storeConf.confs)
    this.isStrictExpire = setExpireMode(storeConf.confs)

    // initialize paths
    remoteBackupPath = stateStoreId.storeCheckpointLocation()
    remoteBackupFs = remoteBackupPath.getFileSystem(hadoopConf)
    remoteBackupFs.setWriteChecksum(false)
    remoteBackupFs.setVerifyChecksum(false)
    localBackupDir = createLocalDir(localDataDir+"/backup")
    localBackupPath = new Path("file:///"+localBackupDir)
    localBackupFs = localBackupPath.getFileSystem(hadoopConf)
    localDbDir = createLocalDir(localDataDir+"/db")

    // initialize empty database
    currentDb = openDb

    // copy backups from remote storage and init backup engine
    val backupDBOptions = new BackupableDBOptions(localBackupDir.toString)
    if (remoteBackupFs.exists(remoteBackupPath)) {
      logDebug(s"loading state backup from remote filesystem $remoteBackupPath")
      val t = measureTime {
        FileUtil.copy(remoteBackupFs, new Path(remoteBackupPath, "shared"), new File(localBackupDir + File.separator + "shared"), false, hadoopConf)
        val archiveFiles = remoteBackupFs.listStatus(remoteBackupPath, new PathFilter {
          override def accept(path: Path): Boolean = path.getName.endsWith(".zip")
        })
        archiveFiles.foreach(f => decompressFromRemote(f.getPath, localBackupDir))
        backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)
      }
      logDebug(s"got state backup from remote filesystem $remoteBackupPath, took $t secs")
    } else {
      logDebug(s"initializing state backup at $remoteBackupFs/$remoteBackupPath")
      remoteBackupFs.mkdirs( new Path(remoteBackupPath, "shared"))
      backupEngine = BackupEngine.open(options.getEnv, backupDBOptions)
      createBackup(0L)
    }
    logInfo(s"initialized $this")
  }

  /**
    * Get the state store for making updates to create a new `version` of the store.
    */
  override def getStore(version: Long): StateStore = getStore(version,createCache(ttlSec))
  def getStore(version: Long, cache: MapType): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    restoreDb(version)
    currentStore = new RocksDbStateStore(version, keySchema, valueSchema, currentDb, cache)
    logInfo(s"Retrieved $currentStore for version $version of ${RocksDbStateStoreProvider.this} for update")
    // return
    currentStore
  }

  protected def restoreDb(version: Long): Unit = {

    // check if current db is desired version, else search in backups and restore
    if (currentStore!=null && currentStore.getCommittedVersion.map(_==version).getOrElse(false)) {
      logDebug(s"Current db already has correct version $version. No restore required.")
    } else {

      // get infos about backup to recover
      val backupInfos = backupEngine.getBackupInfo.asScala
      val backupIdRecovery = backupInfos.find(_.appMetadata.toLong == version).map(_.backupId)
        .getOrElse(throw new IllegalStateException(s"backup for version $version not found"))

      // close db and restore backup
      val tRestore = measureTime {
        // TODO: is closing needed?
        currentDb.close()
        val restoreOptions = new RestoreOptions(false)
        backupEngine.restoreDbFromBackup(backupIdRecovery, localDbDir, localDbDir, restoreOptions)
        currentDb = openDb
      }
      logDebug(s"restored db for version $version from local backup, took $tRestore secs")

      // delete potential newer backups to avoid diverging data versions. See also comment for [[BackupEngine.restoreDbFromBackup]]
      val newerBackups = backupInfos.filter(_.backupId > backupIdRecovery)
      val tCleanup = measureTime {
        newerBackups.foreach(backupInfo => backupEngine.deleteBackup(backupInfo.backupId))
      }
      if (newerBackups.nonEmpty) logDebug(s"deleted newer backups for versions ${newerBackups.map(_.appMetadata()).mkString(", ")}, took $tCleanup secs")
    }
  }

  private def openDb: TtlDB = TtlDB.open(options, localDbDir, ttlSec, false)

  /**
    * Return the id of the StateStores this provider will generate.
    */
  override def stateStoreId: StateStoreId = stateStoreId_

  /**
    * Do maintenance backing data files, including cleaning up old files
    */
  override def doMaintenance(): Unit = {
    // flush WAL to SST Files and do manual compaction
    // TODO: measure time
    val t = measureTime {
      currentDb.flush(new FlushOptions().setWaitForFlush(true))
      currentDb.compactRange()
      // cleanup old backups
      backupEngine.purgeOldBackups(storeConf.minVersionsToRetain)
    }
    logInfo(s"doMaintenance took $t secs")
  }

  def measureTime[T](code2exec: => Unit): Float = {
    val t0 = System.currentTimeMillis()
    code2exec
    (System.currentTimeMillis()-t0).toFloat / 1000
  }

  /**
    * Called when the provider instance is unloaded from the executor.
    */
  override def close(): Unit = {
    deleteFile(localDataDir)
    logInfo(s"Removed local db and backup dir of ${RocksDbStateStoreProvider.this}")
  }

  /**
    * Custom toString implementation for this state store provider class.
    */
  override def toString: String = {
    s"RocksDbStateStoreProvider[" +
      s"id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId}),dir = $remoteBackupPath]"
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

  private def createBackup(version: Long): Unit = {

    // create local backup
    val tBackup = measureTime {
      // Don't flush before backup, as also WAL is backuped. We can use WAL for efficient incremental backup
      backupEngine.createNewBackupWithMetadata(currentDb, version.toString, false) // create backup for version 0
    }
    val backupId = backupEngine.getBackupInfo.asScala.map(_.backupId()).max
    logDebug(s"created backup for version $version with backupId $backupId, took $tBackup secs")

    // sync to remote filesystem
    val tSync = measureTime {
      syncRemoteBackup(backupId, version)
    }
    logDebug(s"synced backup to remote filesystem, took $tSync secs")
  }

  //private def getBackupEngine(): BackupEngine = backupEngine

  private def syncRemoteBackup(backupId: Int, version: Long): Unit = {

    // diff shared data files
    val (sharedFiles2Copy,sharedFiles2Del) = getRemoteSyncList(new Path(remoteBackupPath,"shared"), new Path(localBackupPath,"shared"), _ => true, true )

    // copy new data files
    sharedFiles2Copy.foreach( f =>
      remoteBackupFs.copyFromLocalFile(false, true, f, new Path(remoteBackupPath,"shared"))
    )

    // save metadata as zip archive
    val metadataFile = new File(s"$localBackupDir${File.separator}meta${File.separator}$backupId")
    val privateFiles = new File(s"$localBackupDir${File.separator}private${File.separator}$backupId").listFiles().toSeq
    compress2Remote(metadataFile +: privateFiles, new Path(remoteBackupPath,s"$version.zip"), Some(localBackupDir))

    // update status
    val latestOutputStream = remoteBackupFs.create( new Path(remoteBackupPath,"latest"), true)
    latestOutputStream.writeBytes(s"version: $version, backupId: $backupId")
    latestOutputStream.close()

    // delete old data files
    sharedFiles2Del.foreach( f =>
      remoteBackupFs.delete(f, false)
    )
  }

  private def getRemoteSyncList( remotePath: Path, localPath: Path, nameFilter: String => Boolean, errorOnChange: Boolean ) = {
    val remoteFiles = remoteBackupFs.listStatus(remotePath, new PathFilter {
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
          if (errorOnChange) logError(s"Remote file $rf is different from local file $lf. Overwriting for now.")
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
    * Save files as ZIP archive in HDFS.
    */
  private def compress2Remote(files: Seq[File], archiveFile: Path, baseDir: Option[String] = None): Unit = {
    val buffer = new Array[Byte](hadoopConf.getInt("io.file.buffer.size", 4096))
    val output = new ZipOutputStream(remoteBackupFs.create(archiveFile, true))
    val basePath = baseDir.map( dir => new java.io.File(dir).toPath)
    try {
      files.foreach(file => {
        val input = new FileInputStream(file)
        try {
          val relativeName = basePath.map( path => path.relativize(file.toPath).toString).getOrElse(file.getName)
          output.putNextEntry(new ZipEntry(relativeName))
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
    * Load ZIP archive from HDFS and unzip files.
    */
  private def decompressFromRemote(archiveFile: Path, tgtPath: String): Unit = {
    val buffer = new Array[Byte](hadoopConf.getInt("io.file.buffer.size", 4096))
    val input = new ZipInputStream(remoteBackupFs.open(archiveFile))
    try {
      Iterator.continually(input.getNextEntry)
        .takeWhile(_ != null)
        .foreach(entry => {
          val file = new File(s"$tgtPath${File.separator}${entry.getName}")
          file.getParentFile.mkdirs
          val output = new FileOutputStream(file)
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
    * Get name for local data directory.
    *
    * @return
    */
  private def getDataDirName(sparkJobName: String): String = {
    // we need a random number to allow multiple streaming queries running with different state stores in the same spark job.
    s"spark-$sparkJobName-${stateStoreId_.operatorId}-${stateStoreId_.partitionId}-${stateStoreId_.storeName}-${math.abs(Random.nextInt)}"
  }

  /**
    * Create local data directory.
    */
  private def createLocalDir(path: String): String = {
    val file = new File(path)
    file.delete()
    file.mkdirs()
    file.getAbsolutePath.replace('\\','/')
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
    val maxBackupId = backupEngine.getBackupInfo.asScala.maxBy(_.backupId).appMetadata.toLong
    getStore(maxBackupId).iterator()
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

  final val STATE_LOCAL_DIR: String = "spark.sql.streaming.stateStore.localDir"

  final val DEFAULT_STATE_LOCAL_DIR: String = System.getProperty("java.io.tmpdir").replace('\\','/')

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

  private def setLocalDir(conf: Map[String, String]): String =
    Try(conf.getOrElse(STATE_LOCAL_DIR, DEFAULT_STATE_LOCAL_DIR)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(e)
    }
}

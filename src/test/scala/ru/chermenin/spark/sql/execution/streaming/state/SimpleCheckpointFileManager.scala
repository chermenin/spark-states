package ru.chermenin.spark.sql.execution.streaming.state

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * An implementation of [[CheckpointFileManager]] which is not renamed based, as on windows we cannot overwrite files written in the same process again.
  * This is an old bug which might be solved in Java 10 or 11, see https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
  * Attention: This should only be used for testing...
  */
class SimpleCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends CheckpointFileManager with Logging {

  private val fs = path.getFileSystem(hadoopConf)
  fs.setWriteChecksum(false)
  fs.setVerifyChecksum(false)

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = fs.listStatus(path, filter)

  override def mkdirs(path: Path): Unit = fs.mkdirs(path, FsPermission.getDirDefault)

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    logInfo(s"Writing $path")
    new SimpleFSDataOutputStream(fs, path, overwriteIfPossible)
  }

  override def open(path: Path): FSDataInputStream = {
    logDebug(s"Reading $path")
    fs.open(path)
  }

  override def exists(path: Path): Boolean = {
    try
      return fs.getFileStatus(path) != null
    catch {
      case e: FileNotFoundException =>
        return false
    }
  }

  override def delete(path: Path): Unit = {
    try {
      fs.delete(path, true)
    } catch {
      case e: FileNotFoundException =>
        logInfo(s"Failed to delete $path as it does not exist")
      // ignore if file has already been deleted
    }
  }

  override def isLocal: Boolean = false // dont know...

  /**
    * An implementation of [[CancellableFSDataOutputStream]] that writes files without rename
    */
  class SimpleFSDataOutputStream( fs: FileSystem, path: Path, overwriteIfPossible: Boolean)
    extends CancellableFSDataOutputStream(fs.create(path, overwriteIfPossible)) {

    @volatile private var terminated = false

    override def close(): Unit = synchronized {
      try {
        if (terminated) return
        underlyingStream.close()
      } finally {
        terminated = true
      }
    }

    override def cancel(): Unit = synchronized {
      underlyingStream.close()
      fs.delete(path, false)
      terminated = true
    }
  }
}
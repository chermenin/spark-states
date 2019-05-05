package ru.chermenin.spark.sql.execution.streaming.state

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.net.InetAddress
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.CheckpointFileManager

import scala.util.Random

object MiscHelper {

  def measureTime[T](code2exec: => Unit): Float = {
    val t0 = System.currentTimeMillis()
    code2exec
    (System.currentTimeMillis()-t0).toFloat / 1000
  }

  def formatBytes(bytes:Long): String = {
    val megaBytes = BigDecimal(bytes.toFloat / 1024 / 1024).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    s"${megaBytes}MB"
  }

  /**
    * Save files as ZIP archive in HDFS.
    */
  def compress2Remote(files: Seq[File], archiveFile: Path, remoteBackupFm: CheckpointFileManager, bufferSize:Int, baseDir: Option[String] = None): Unit = {
    val buffer = new Array[Byte](bufferSize)
    val output = new ZipOutputStream(remoteBackupFm.createAtomic(archiveFile, true))
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
  def decompressFromRemote(archiveFile: Path, tgtPath: String, remoteBackupFm: CheckpointFileManager, bufferSize:Int): Unit = {
    val buffer = new Array[Byte](bufferSize)
    val input = new ZipInputStream(remoteBackupFm.open(archiveFile))
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
    * Create local data directory.
    */
  def createLocalDir(path: String): String = {
    val file = new File(path)
    FileUtils.deleteQuietly(file)
    file.mkdirs()
    file.getAbsolutePath.replace('\\','/')
  }

  /**
    * get local host name without domain
    */
  def getHostName: String = {
    InetAddress.getLocalHost.getHostName.takeWhile(c => c!='.')
  }

  /**
    * get random integer of globally initialized Random object
    */
  private val randomInt = Random // init Random object
  def getRandomPositiveInt: Int = {
    math.abs(Random.nextInt())
  }

  /**
    * Verify the condition and rise an exception if the condition is failed.
    */
  def verify(condition: => Boolean, msg: String): Unit =
    if (!condition) throw new IllegalStateException(msg)

  /**
    * maps number to string composed of A-Z
    */
  def nbToChar(nb: Int) = {
    val chars = 'A' to 'Z'
    def nbToCharRecursion( nb: Int ): String = {
      val res = nb/chars.size
      val rem = nb%chars.size
      if(res>0) nbToCharRecursion(res) else "" +chars(rem)
    }
    nbToCharRecursion(nb)
  }

  /**
    * check if running on Windows OS
    */
  def isWindowsOS = System.getProperty("os.name").toLowerCase.contains("windows")
}

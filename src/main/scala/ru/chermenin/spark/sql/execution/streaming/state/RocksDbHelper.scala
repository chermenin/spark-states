package ru.chermenin.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair
import org.apache.spark.sql.types.StructType
import org.rocksdb.RocksDB

import scala.io.Source

object RocksDbHelper {

  def parseSharedFilesFromMetadata(backupDir: String, backupId: Int): Seq[String] = {
    val metaFile = backupDir + "/meta/" + backupId
    val sstRegex = "[0-9_]*\\.sst".r
    Source.fromFile(metaFile).getLines().flatMap( l => sstRegex.findFirstIn(l)).toList
  }

  def getIterator(db: RocksDB, keySchema: StructType, valueSchema: StructType ): Iterator[UnsafeRowPair] = {
    new Iterator[UnsafeRowPair] {

      /** Internal RocksDb iterator */
      private val iterator = db.newIterator()
      iterator.seekToFirst()

      /** Check if has some data */
      override def hasNext: Boolean = {
        if (iterator.isValid) true
        else {
          iterator.close // close when end of iterator reached (otherwise there might be native memory leaks...)
          false
        }
      }

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
  }

}

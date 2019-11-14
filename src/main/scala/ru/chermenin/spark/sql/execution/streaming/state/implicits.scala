package ru.chermenin.spark.sql.execution.streaming.state

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.DataStreamWriter
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._

import scala.collection.mutable

/**
  * Implicits aka helper methods
  *
  * The can be imported into scope with ,
  * import ru.chermenin.spark.sql.execution.streaming.state.implicits._
  *
  * SessionImplicits:
  *   - Makes the `useRocksDBStateStore` method available on [[Builder]]
  *   - Sets provider to [[RocksDbStateStoreProvider]]
  *
  * WriterImplicits:
  *   - Makes the `stateTimeout` method available on [[DataStreamWriter]]
  *   - Precedence is given to the provided arguments (if any), then previously set value,
  *   and finally the value set on [[RuntimeConfig]] (in case of checkpoint location)
  *   - Makes Checkpoint mandatory for all query on which applied
  *   - Expiry Seconds less than 0 are treated as -1 (no timeout)
  */

object implicits extends Serializable {

  implicit class SessionImplicits(sparkSessionBuilder: Builder) {

    def useRocksDBStateStore(): Builder =
      sparkSessionBuilder.config(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        classOf[RocksDbStateStoreProvider].getCanonicalName)

  }

  implicit class WriterImplicits[T](dsw: DataStreamWriter[T]) {

    def stateTimeout(runtimeConfig: RuntimeConfig,
                     queryName: String = "",
                     expirySecs: Int = DEFAULT_STATE_EXPIRY_SECS.toInt,
                     checkpointLocation: String = ""): DataStreamWriter[T] = {

      val extraOptions = getExtraOptions
      val name = queryName match {
        case "" | null => extraOptions.getOrElse("queryName", UNNAMED_QUERY)
        case _ => queryName
      }

      val location = new Path(checkpointLocation match {
        case "" | null =>
          extraOptions.getOrElse("checkpointLocation",
            runtimeConfig.getOption(SQLConf.CHECKPOINT_LOCATION.key
            ).getOrElse(throw new IllegalStateException(
              "Checkpoint Location must be specified for State Expiry either " +
                """through option("checkpointLocation", ...) or """ +
                s"""SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)"""))
          )
        case _ => checkpointLocation
      }, name)
        .toUri.toString

      runtimeConfig.set(s"$STATE_EXPIRY_SECS.$name", if (expirySecs < 0) -1 else expirySecs)

      dsw
        .queryName(name)
        .option("checkpointLocation", location)
    }

    private def getExtraOptions: mutable.HashMap[String, String] = {
      val className = classOf[DataStreamWriter[T]]
      val field = className.getDeclaredField("extraOptions")
      field.setAccessible(true)

      field.get(dsw).asInstanceOf[mutable.HashMap[String, String]]
    }
  }

}

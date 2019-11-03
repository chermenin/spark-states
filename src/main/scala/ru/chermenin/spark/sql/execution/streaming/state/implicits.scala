package ru.chermenin.spark.sql.execution.streaming.state

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.DataStreamWriter
import ru.chermenin.spark.sql.execution.streaming.state.RocksDbStateStoreProvider._

import scala.collection.mutable

object implicits extends Serializable {

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

    def getExtraOptions: mutable.HashMap[String, String] = {
      val className = classOf[DataStreamWriter[T]]
      val field = className.getDeclaredField("extraOptions")
      field.setAccessible(true)

      field.get(dsw).asInstanceOf[mutable.HashMap[String, String]]
    }
  }

  implicit class SessionImplicits(sparkSessionBuilder: Builder) {

    def useRocksDBStateStore(): Builder =
      sparkSessionBuilder.config(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        classOf[RocksDbStateStoreProvider].getCanonicalName)

  }


}

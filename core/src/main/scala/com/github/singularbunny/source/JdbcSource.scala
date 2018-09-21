package com.github.singularbunny.source

import com.github.singularbunny.param.HasJdbcParams
import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}

class JdbcSource(override val uid: String)(implicit spark: SparkSession) extends Source
  with HasJdbcParams
  with Logging {

  def this()(implicit spark: SparkSession) = this(Identifiable.randomUID("jdbc_source"))

  override def getSourceDataFrame: DataFrame = {
    log.info(s"Read from DB with " +
      s"${$(jdbcUrl)} JDBC URL " +
      s"${$(dbTableName)} DB table name " +
      s"${$(properties)} properties")

    if ($(predicates).nonEmpty) {
        log.info(s"${$(predicates).mkString(", ")} predicates")
        spark
          .read
          .jdbc($(jdbcUrl),
            $(dbTableName),
            $(predicates),
            $(properties)
          )
      } else {
      spark
        .read
        .jdbc($(jdbcUrl),
          $(dbTableName),
          $(properties)
        )
    }
  }
}

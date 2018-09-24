package com.github.singularbunny.source

import com.github.singularbunny.param.{HasFormat, HasOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}

class HdfsSource(override val uid: String)(implicit spark: SparkSession) extends Source
  with HasFormat
  with HasOptions
  with Logging {

  def this()(implicit spark: SparkSession) = this(Identifiable.randomUID("hdfs_source"))

  override def getSourceDataFrame: DataFrame = {
    log.info(s"Read from HDFS with " +
      s"${$(options)} format " +
      s"${$(options)} options")

    spark
      .read
      .format($(format))
      .options($(options))
      .load()
  }
}

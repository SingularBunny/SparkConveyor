package com.github.singularbunny.receiver

import com.github.singularbunny.param.{HasFormat, HasMode, HasOptions, HasSparkTableName}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}

class HiveReceiver(override val uid: String)(implicit spark: SparkSession) extends Receiver
  with HasOptions
  with HasFormat
  with HasMode
  with HasSparkTableName
  with Logging {

  def this()(implicit spark: SparkSession) = this(Identifiable.randomUID("hive_source"))

  override def putDataFrame(dataFrame: DataFrame): DataFrame = {
    log.info(s"Write to Hive with " +
      s"${$(format)} format " +
      s"${$(options)} options " +
      s"${$(mode)} mode and " +
      s"${$(sparkTableName)} table name")

    dataFrame.write
      .format($(format))
      .options($(options))
      .mode($(mode))
      .saveAsTable($(sparkTableName))
    dataFrame
  }
}

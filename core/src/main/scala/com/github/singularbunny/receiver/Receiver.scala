package com.github.singularbunny.receiver

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

trait Receiver extends Transformer {

  def putDataFrame(dataFrame: DataFrame): DataFrame

  override def transform(dataset: Dataset[_]): DataFrame = {
    putDataFrame(dataset.toDF())
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
  override def copy(extra: ParamMap): SQLTransformer = defaultCopy(extra)
}

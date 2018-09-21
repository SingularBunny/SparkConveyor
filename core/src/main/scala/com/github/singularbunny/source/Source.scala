package com.github.singularbunny.source

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

trait Source extends Transformer {

  def getSourceDataFrame: DataFrame

  override def transform(dataset: Dataset[_]): DataFrame = {
    getSourceDataFrame
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
  override def copy(extra: ParamMap): SQLTransformer = defaultCopy(extra)
}

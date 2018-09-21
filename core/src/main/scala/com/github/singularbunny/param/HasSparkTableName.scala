package com.github.singularbunny.param

import org.apache.spark.ml.param.{Param, Params}

trait HasSparkTableName extends Params {

  final val sparkTableName: Param[String] = new Param[String](this, "sparkTableName", "Table name where to save")

  final def getSparkTableName: String = $(sparkTableName)

  final def setSparkTableName(value: String): this.type = set(sparkTableName, value)
}

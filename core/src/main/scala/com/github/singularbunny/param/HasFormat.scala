package com.github.singularbunny.param

import org.apache.spark.ml.param.{Param, Params}

trait HasFormat extends Params {

  final val format: Param[String] = new Param[String](this, "format", "format to save")

  final def getFormat: String = $(format)

  final def setFormat(value: String): this.type = set(format, value)
}

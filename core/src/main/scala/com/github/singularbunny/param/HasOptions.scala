package com.github.singularbunny.param

import org.apache.spark.ml.param.{Param, Params}

trait HasOptions extends Params {

  final val options: Param[Map[String, String]] =
    new Param[Map[String, String]](this, "options", "Spark read or write options")

  final def getOptions: Map[String, String] = $(options)

  final def setOptions(value: Map[String, String]): this.type = set(options, value)
}

package com.github.singularbunny.param

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.SaveMode

trait HasMode extends Params {

  final val mode: Param[SaveMode] = new Param[SaveMode](this, "mode", "Save mode")

  final def getMode: SaveMode = $(mode)

  final def setMode(value: SaveMode): this.type = set(mode, value)
}

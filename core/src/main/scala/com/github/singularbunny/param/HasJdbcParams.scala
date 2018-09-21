package com.github.singularbunny.param

import java.util.Properties

import org.apache.spark.ml.param.{Param, Params}

trait HasJdbcParams extends Params {

  final val properties: Param[Properties] =
    new Param[Properties](this, "properties", "Spark JDBC properties")

  final val predicates: Param[Array[String]] =
    new Param[Array[String]](this, "predicates", "Filtering conditions like \"col > 23\"")

  final val jdbcUrl: Param[String] = new Param[String](this, "jdbcUrl", "JDBC URL")

  final val dbTableName: Param[String] =
    new Param[String](this, "dbTableName", "Table name where to save")

  final def getProperties: Properties = $(properties)

  final def setProperties(value: Properties): this.type = set(properties, value)

  final def getPredicates: Array[String] = $(predicates)

  final def setPredicates(value: Array[String]): this.type = set(predicates, value)

  final def getJdbcUrl: String = $(jdbcUrl)

  final def setJdbcUrl(value: String): this.type = set(jdbcUrl, value)

  final def getDbTableName: String = $(dbTableName)

  final def setDbTableName(value: String): this.type = set(dbTableName, value)
}

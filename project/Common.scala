import sbt.Keys.{test, _}
import sbt._
import sbt.{Resolver, _}

object Common {

  private val SCALA_VERSION = "2.11.8"

  // Use the prefix for artifactory jars in the project
  val prefix: String = "sc-"

  val commonSettings: Seq[Def.Setting[_]] = Seq(
    resolvers := Seq(
      "mvnrepository-repository" at "https://mvnrepository.com/artifact/",
      "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/",
      "Hortonworks Jetty Repository" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/",
      Resolver.sonatypeRepo("releases"),
      "Maven Atlassian" at "https://maven.atlassian.com/3rdparty/"),
    parallelExecution := false,
    scalaVersion := SCALA_VERSION,
    updateOptions := updateOptions.value.withCachedResolution(true)
  )
}

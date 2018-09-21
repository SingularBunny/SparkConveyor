import sbt.ExclusionRule

val HDP_MINOR_VERSION = "2.6.4.9-3"

val SPARK_VERSION = "2.2.0" + "." + HDP_MINOR_VERSION
val JACKSON_VERSION = "2.6.5"

lazy val commonDependencies = Seq(
  "org.scalactic" %% "scalactic" % "2.2.6" % Test,
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.mockito" % "mockito-core" % "2.13.0" % Test
)

lazy val core = (project in file("core"))
  .settings(Common.commonSettings)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % SPARK_VERSION % Provided,
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % Provided,
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % Test,
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % Test classifier "tests",
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % Provided,
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % Test,
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % Test classifier "tests",
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % Provided,
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % Test,
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % Test classifier "tests"))

lazy val root = (project in file("."))
  .settings(Common.commonSettings)
  .aggregate(core)

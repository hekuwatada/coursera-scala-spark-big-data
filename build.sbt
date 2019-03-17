name := "coursera-scala-spark-big-data"

version := "0.1"

// Spark internally has Scala
// Spark 2.4.0 uses Scala 2.11.12, Java 1.8.0
// Recommended to use Scala 2.11.x and SBT 0.13.x
scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

val prodLibs = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion // required for SparkSession
)

val scalaTestVersion = "3.0.5"

val testLibs = Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalactic" %% "scalactic" % scalaTestVersion % Test
)

//TODO: review JVM memory size
//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

// By default, the run task runs in the same JVM as sbt.
// By default, a forked process uses the same Java and Scala versions
// being used for the build and the working directory and JVM options of
// the current process.
//@see https://www.scala-sbt.org/0.13/docs/Forking.html
//TODO: set to jdk 1.8 ?
fork in (Test, run) := true

//TODO: review parallel execution of tests
parallelExecution in Test := false

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= prodLibs ++ testLibs
  )
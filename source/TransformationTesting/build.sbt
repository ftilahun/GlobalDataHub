// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// Project name
name := """transformation"""

// Don't forget to set the version
version := "0.1-SNAPSHOT"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// scala version to be used
scalaVersion := "2.11.8"
// force scalaVersion
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// spark version to be used
val sparkVersion = "1.6.2"

// Needed as SBT's classloader doesn't work well with Spark
fork := true

// BUG: unfortunately, it's not supported right now
fork in console := true

fork in Test := true

// Java version
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// add a JVM option to use when forking a JVM for 'run'
javaOptions ++= Seq("-Xms512M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-unchecked")

// Use local repositories by default
resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  // For Typesafe goodies, if not available through maven
  // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  // For Spark development versions, if you don't want to build spark yourself
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
)

// copy all dependencies into lib_managed/
//retrieveManaged := true

// scala modules (should be included by spark, just an exmaple)
//libraryDependencies ++= Seq(
//  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
//  "org.scala-lang" % "scala-compiler" % scalaVersion.value
//  )

val sparkDependencyScope = "compile"

// spark modules (should be included by spark-sql, just an example)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkDependencyScope
)

// spark-avro
libraryDependencies ++= Seq(
  "com.databricks" %% "spark-avro" % "2.0.1",
  "org.reflections" % "reflections" % "0.9.10",
  "com.github.scopt" %% "scopt" % "3.5.0"
)

// logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalactic" %% "scalactic" % "2.2.4" % "test",
  // exclude scalacheck here because sbt defaults to using it to run tests
  "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.3" % "test" exclude("org.scalacheck", "scalacheck_2.11"),
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)


/// Compiler plugins

// linter: static analysis for scala
// resolvers += "Linter Repository" at "https://hairyfotr.github.io/linteRepo/releases"

// addCompilerPlugin("com.foursquare.lint" %% "linter" % "0.1.8")


/// console

// define the statements initially evaluated when entering 'console', 'consoleQuick', or 'consoleProject'
// but still keep the console settings in the sbt-spark-package plugin

// If you want to use yarn-client for spark cluster mode, override the environment variable
// SPARK_MODE=yarn-client <cmd>
val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")

initialCommands in console :=
  s"""
     |import org.apache.spark.SparkConf
     |import org.apache.spark.SparkContext
     |import org.apache.spark.SparkContext._
     |
    |@transient val sc = new SparkContext(
     |  new SparkConf()
     |    .setMaster("$sparkMode")
     |    .setAppName("Console test"))
     |implicit def sparkContext = sc
     |import sc._
     |
    |@transient val sqlc = new org.apache.spark.sql.SQLContext(sc)
     |implicit def sqlContext = sqlc
     |import sqlc._
     |
    |def time[T](f: => T): T = {
     |  import System.{currentTimeMillis => now}
     |  val start = now
     |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
     |}
     |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |sc.stop()
   """.stripMargin

autoAPIMappings := true

parallelExecution in Test := false

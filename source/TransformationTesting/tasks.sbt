
val sourceSystems = List("Ndex", "Genius", "Eclipse")

def suiteTemplate(mappingName: String, sourceSystem: String, feedTableNames: Seq[(String, Seq[String])]) =
  s"""
    |package com.kainos.enstar.transformation.${sourceSystem.toLowerCase}
    |
    |import com.kainos.enstar.transformation._
    |
    |class ${mappingName}QuerySuite extends QuerySuite {
    |
    |  val sourceType = sourcetype.${sourceSystem}
    |
    |  override def testTags = List( tags.${mappingName} )
    |
    |  override def queryTestSets : List[QueryTestSet] = List(
    |${feedTableNames.map { case (feedName, feedTables) => queryTestSetTemplate(mappingName, feedName, feedTables) }.mkString(",\n")}
    |  )
    |}
  """.stripMargin.getBytes

def queryTestSetTemplate(mappingName: String, feedName: String, feedTableNames: Seq[String]) =
  s"""
     |    QueryTestSet(
     |      "${mappingName}${ if (feedName != "DEFAULT") feedName else "" }",
     |      "${mappingName.toLowerCase}${ if (feedName != "DEFAULT") "/" + feedName.toLowerCase else "" }",
     |      "${mappingName}${ if (feedName != "DEFAULT") feedName else "" }.hql",
     |      Set(
     |        QueryTest(
     |          "Primary",
     |          Set(
     |${feedTableNames map csvSourceDataTemplate mkString (",\n")}
     |          ),
     |          CsvSourceData( "${mappingName.toLowerCase}", "PrimaryTestData.csv" )
     |        )
     |      )
     |    )
     |
   """.stripMargin

def csvSourceDataTemplate(tableName: String) = s"""            CsvSourceData( "${tableName}", "PrimaryTestData.csv" )"""

lazy val createMapping = taskKey[Unit]("Creates boilerplate mapping files and test cases")

createMapping := {
  val log = streams.value.log

  val defaultFeedName = "DEFAULT"

  // read config from input
  val sourceSystem = readSourceSystem
  val mappingName = readMappingName
  val inputFeedNames = readFeedNames

  val feedNames = if (inputFeedNames.isEmpty) Seq(defaultFeedName) else inputFeedNames

  val feedTableNames = feedNames.map { feed =>
    feed -> readTableNames(feed)
  }

  // create directories and files
  val src = sourceDirectory.value
  val mappingDir = src / "main" / "resources" / "Transformation" / sourceSystem.toLowerCase
  sbt.IO.createDirectory(mappingDir)

  val testResourceDir = src / "test" / "resources" / sourceSystem.toLowerCase / mappingName.toLowerCase
  sbt.IO.createDirectory(testResourceDir)

  for ((feed, feedTables) <- feedTableNames.toList) {
    val queryFileName = if (feed == defaultFeedName) s"$mappingName.hql" else s"$mappingName$feed.hql"
    val queryFile = mappingDir / queryFileName
    sbt.IO.touch(queryFile)
    log.info(s"Created $queryFile")

    val dir = if (feed == defaultFeedName) testResourceDir else testResourceDir / feed.toLowerCase
    for (tableName <- feedTables) {
      sbt.IO.createDirectory(dir / "input" / tableName)
      val inputDataFile = dir / "input" / tableName / "PrimaryTestData.csv"
      sbt.IO.touch(inputDataFile)
      log.info(s"Created $inputDataFile")
    }
    sbt.IO.createDirectory(dir / "output")
    val outputDataFile = dir / "output" / "PrimaryTestData.csv"
    sbt.IO.touch(outputDataFile)
    log.info(s"Created $outputDataFile")
  }

  val testSourceDir = src / "test" / "scala" / "com" / "kainos" / "enstar" / "transformation" / sourceSystem.toLowerCase
  val testSourceFile = testSourceDir / s"${mappingName}QuerySuite.scala"
  if (!testSourceFile.exists) {
    sbt.IO.write(testSourceFile, suiteTemplate(mappingName, sourceSystem, feedTableNames))
    log.info(s"Created $testSourceFile")
  }
}

def readSourceSystem: String = {
  println(s"\nEnter source system (1 - ${sourceSystems.length})")
  for ((sourceSystem, index) <- sourceSystems.zipWithIndex) {
    println(s"  (${index + 1}) $sourceSystem")
  }
  val sourceIndex = readLine.toInt - 1
  sourceSystems(sourceIndex)
}

def readMappingName: String = {
  println("\nEnter mapping name in UpperCamelCase:")
  val name = readLine
  name
}

def readFeedNames: Seq[String] = {
  println(s"\nEnter feed names (empty line to finish, leave empty if single feed):")
  val names = readList
  names
}

def readTableNames(feed: String): Seq[String] = {
  println(s"\nEnter source table names for feed $feed as they appear in source system (empty line to finish):")
  val tables = readList
  tables
}

def readLine: String = {
  print("> ")
  scala.io.Source.fromInputStream(System.in).bufferedReader().readLine.trim
}

def readList: Seq[String] = {
  var names = List[String]()
  do {
    names = readLine :: names
  } while (!names.head.isEmpty)
  names.tail.reverse
}


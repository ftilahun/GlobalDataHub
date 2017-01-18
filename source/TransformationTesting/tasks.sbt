
val sourceSystems = List("Ndex", "Genius", "Eclipse")

def suiteTemplate(mappingName: String, sourceSystem: String, tableNames: Seq[String]) =
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
    |    QueryTestSet(
    |      "${mappingName}",
    |      "${mappingName.toLowerCase}",
    |      "${mappingName}.hql",
    |      Set(
    |        QueryTest(
    |          "Primary",
    |          Set(
    |${tableNames map csvSourceDataTemplate mkString(",\n")}
    |          ),
    |          CsvSourceData( "${mappingName.toLowerCase}", "PrimaryTestData.csv" )
    |        )
    |      )
    |    )
    |  )
    |}
  """.stripMargin.getBytes

def csvSourceDataTemplate(tableName: String) = s"""            CsvSourceData( "${tableName}", "PrimaryTestData.csv" )"""

lazy val createMapping = taskKey[Unit]("Creates boilerplate mapping files and test cases")

createMapping := {
  val log = streams.value.log
  val sourceSystem = readSourceSystem
  val mappingName = readMappingName
  val tableNames = readTableNames

  val src = sourceDirectory.value
  val mappingDir = src / "main" / "resources" / "Transformation" / sourceSystem.toLowerCase

  sbt.IO.createDirectory(mappingDir)
  sbt.IO.touch(mappingDir / s"$mappingName.hql")

  val testResourceDir = src / "test" / "resources" / sourceSystem.toLowerCase / mappingName.toLowerCase

  sbt.IO.createDirectory(testResourceDir)
  for (tableName <- tableNames) {
    sbt.IO.createDirectory(testResourceDir / "input" / tableName)
    sbt.IO.touch(testResourceDir / "input" / tableName / "PrimaryTestData.csv")
  }
  sbt.IO.createDirectory(testResourceDir / "output")
  sbt.IO.touch(testResourceDir / "output" / "PrimaryTestData.csv")

  val testSourceDir = src / "test" / "scala" / "com" / "kainos" / "enstar" / "transformation" / sourceSystem.toLowerCase
  val testSourceFile = testSourceDir / s"${mappingName}QuerySuite.scala"
  if (!testSourceFile.exists) {
    sbt.IO.write(testSourceFile, suiteTemplate(mappingName, sourceSystem, tableNames))
  }

  log.info(s"Source System: $sourceSystem")
  log.info(s"Mapping Name:  $mappingName")
  for (tableName <- tableNames) {
    log.info(s"Table:         $tableName")
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

def readUnionQueryNames: Seq[String] = {
  println(s"\nEnter union query names (empty line to finish, leave empty if single query):")
  val names = readList
  names
}

def readTableNames: Seq[String] = {
  println(s"\nEnter source table names as they appear in source system (empty line to finish):")
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


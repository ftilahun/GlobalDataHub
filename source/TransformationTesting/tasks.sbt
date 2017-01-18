import java.io.File
import sbt.complete._

lazy val createMapping = taskKey[Seq[File]]("Creates boilerplate mapping files and test cases")

lazy val tableName = inputKey[String]("Enter a table name")

val tableNameParser: Parser[String] = {
  import complete.DefaultParsers._
  token(any.* map (_.mkString))
}

createMapping := {
  val log = streams.value.log
  log.info("creating mapping")
  Seq()
}

tableName := {
  val log = streams.value.log
  val name = tableNameParser.parsed
  log.info("entered: " + name)
  name
}
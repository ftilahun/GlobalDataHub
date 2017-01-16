package enstar.cdcprocessor.properties

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import enstar.cdcprocessor.exceptions.PropertyNotSetException

/**
 * Unit tests for CommandLinePropertyParser
 */
class CDCProcessorCommandLinePropertyParserSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CommandLinePropertyParser" should "Parse the command line arguments" in {
    Given("A set of command line args")
    val argsArray = Array[String](
      "--changeInput",
      "c",
      "--activeOutput",
      "a",
      "--idColumnName",
      "i",
      "--transactionIdColumnName",
      "t",
      "--changeSequenceColumnName",
      "c",
      "--attunityColumnPrefix",
      "header__",
      "--operationColumnName",
      "B",
      "--operationColumnValueBefore",
      "B",
      "--operationColumnValueInsert",
      "B",
      "--operationColumnValueUpdate",
      "B",
      "--operationColumnValueDelete",
      "B",
      "--validFromColumnName",
      "A",
      "--validToColumnName",
      "A",
      "--transactionTimeStampColumnName",
      "A",
      "--activeColumnName",
      "A",
      "--timeWindowInHours",
      "2",
      "--attunityDateFormat",
      "A",
      "--historyInput",
      "A",
      "--immatureChangesOutput",
      "A",
      "--historyOutput",
      "A",
      "--attunityDateFormatShort",
      "A"
    )
    When("The argument list is complete")
    val parser = new CommandLinePropertyParser
    val props = parser.parse(argsArray)
    Then("A properties object should be created")

    props.activeOutput should be("a")
    props.changeInputDir should be("c")
    props.changeSequenceColumnName should be("c")
    props.idColumnName should be("i")
    props.transactionIdColumnName should be("t")
  }

  "CommandLinePropertyParser" should "Throw when unable to parse command line options" in {
    Given("A set of command line args")
    val argsArray = Array[String](
      "--changeInput",
      "c",
      "--activeOutput",
      "a",
      "--idColumnName",
      "i",
      "--transactionIdColumnName",
      "t"
    )
    When("The argument list is incomplete")
    val parser = new CommandLinePropertyParser
    Then("An exception should be thrown")
    an[PropertyNotSetException] should be thrownBy {
      parser.parse(argsArray)
    }
  }
}

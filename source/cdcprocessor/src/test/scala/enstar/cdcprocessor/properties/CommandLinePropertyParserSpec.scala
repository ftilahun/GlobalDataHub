package enstar.cdcprocessor.properties

import enstar.cdcprocessor.exceptions.PropertyNotSetException
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CommandLinePropertyParser
 */
class CommandLinePropertyParserSpec
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
      "header__"
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

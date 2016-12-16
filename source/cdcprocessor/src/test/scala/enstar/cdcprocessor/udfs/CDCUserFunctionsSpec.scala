package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.TestContexts
import enstar.cdcprocessor.properties.CDCProperties
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.mockito.mock.SerializableMode
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCUserFunctions
 */
class CDCUserFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCUserFunctions" should "Return the current time in hive format" in {
    Given("A date")
    val userFunctions = new CDCUserFunctions
    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed(date.getMillis)
    Then("The date should me formated to match")
    userFunctions.getCurrentTime("YYYY-MM-DD HH:mm:ss.SSS") should
      be(
        DateTimeFormat.forPattern("YYYY-MM-DD HH:mm:ss.SSS").print(date)
      )
    DateTimeUtils.setCurrentMillisSystem()
  }

  "CDCUserFunctions" should "Group changes by transaction" in {
    val tableName = "Policy"
    val userFunctions = new CDCUserFunctions
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito
      .when(
        properties.transactionColumnName)
      .thenReturn("_transaction")
    Mockito
      .when(
        properties.idColumnName)
      .thenReturn("id")

    Mockito
      .when(
        properties.changeSequenceColumnName)
      .thenReturn("_changesequence")

    userFunctions
      .groupByTransactionAndKey(TestContexts.changeDummyData(10),
        properties,
        tableName)
      .count should be(10)
  }
}

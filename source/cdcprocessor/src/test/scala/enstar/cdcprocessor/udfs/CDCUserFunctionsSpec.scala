package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.TestContexts
import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.AnalysisException
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

    val userFunctions = new CDCUserFunctions
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito
      .when(
        properties.transactionColumnName)
      .thenReturn("header__transaction")
    Mockito
      .when(
        properties.idColumnName)
      .thenReturn("id")

    Mockito
      .when(
        properties.changeSequenceColumnName)
      .thenReturn("header__changesequence")

    userFunctions
      .groupByTransactionAndKey(TestContexts.changeDummyData(10),
        properties)
      .count should be(10)
  }

  "CDCUserFunctions" should "Drop attunity columns" in {
    val userFunctions = new CDCUserFunctions
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    Mockito
      .when(
        properties.attunityColumnPrefix)
      .thenReturn("header__")

    an[AnalysisException] should be thrownBy {
      userFunctions.dropAttunityColumns(TestContexts.changeDummyData(10),
        properties).select("header__transaction")
    }
  }
}

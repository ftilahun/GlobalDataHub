package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.{ GeneratedData, TestContexts }
import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.mockito.mock.SerializableMode
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import org.apache.spark.sql.functions.udf

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
      .when(properties.transactionIdColumnName)
      .thenReturn("header__transaction")

    Mockito
      .when(properties.changeSequenceColumnName)
      .thenReturn("header__changesequence")

    Mockito.when(properties.idColumnName).thenReturn("id")

    Given("a user functions object")
    When("no rows are present in the dataframe")
    Then("No rows should be returned")
    userFunctions
      .groupByTransactionAndKey(TestContexts.changeDummyData(0), properties)
      .count should be(0)

    Given("a user functions object")
    When(
      "There are 10 rows each with a distinct transaction id and business id")
    Then("10 rows should be returned")
    userFunctions
      .groupByTransactionAndKey(TestContexts.changeDummyData(10), properties)
      .count should be(10)

    Given("a user functions object")
    When("There are 10 rows in a single transaction for a single business id")
    val data1 = TestContexts.changeDummyData(10)
    val transactionid = udf(() => "ONE")
    val id = udf(() => 1)
    Then("1 row should be returned")
    userFunctions
      .groupByTransactionAndKey(
        data1
          .drop(data1("header__transaction"))
          .drop(data1("id"))
          .withColumn("id", id())
          .withColumn("header__transaction", transactionid()),
        properties)
      .count should be(1)

    Given("a user functions object")
    When("There are 10 rows in a single transaction for two business ids")
    Mockito.when(properties.idColumnName).thenReturn("newid")
    val data = TestContexts.changeDummyData(10)
    val transactionid2 = udf(() => "ONE")
    val id2 = udf((id: Int) => if (id < 5) 1 else 2)
    Then("2 rows should be returned")
    userFunctions
      .groupByTransactionAndKey(
        data
          .drop(data("header__transaction"))
          .withColumn("header__transaction", transactionid2())
          .withColumn("newid", id2(data("id"))),
        properties)
      .count should be(2)
  }

  "CDCUserFunctions" should "Drop attunity columns" in {

    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    Mockito.when(properties.attunityColumnPrefix).thenReturn("header__")
    Given("a user functions object")
    val userFunctions = new CDCUserFunctions

    When("There are rows to be processed")
    val data = TestContexts.changeDummyData(10)

    Then("attunity columns should be dropped")
    an[scala.MatchError] should be thrownBy {
      data.collect().map {
        case Row(id: Int, value: String) => GeneratedData(id, value)
      }
    }

    userFunctions
      .dropAttunityColumns(data, properties)
      .collect()
      .map {
        case Row(id: Int, value: String) => GeneratedData(id, value)
      }
      .length should be(10)

    When("There are no rows to be processed")
    val nodata = TestContexts.changeDummyData(0)
    Then("No errors should occur")
    userFunctions
      .dropAttunityColumns(nodata, properties)
      .collect()
      .map {
        case Row(id: Int, value: String) => GeneratedData(id, value)
      }
      .length should be(0)
  }
}

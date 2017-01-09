package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.{ GeneratedData, TestContexts }
import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.{ AnalysisException, Row }
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.mockito.mock.SerializableMode
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import org.apache.spark.sql.functions._

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

    Given("a user functions object")
    When("The attunity columns are not present")
    Then("An exception should be raised")
    an[AnalysisException] should be thrownBy {
      userFunctions.groupByTransactionAndKey(TestContexts.dummyData(10), properties)
    }
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
      .dropAttunityColumns(data, properties).
      drop("validfrom").
      drop("validto")
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

  "CDCUserFunctions" should "sort a dataframe by business key" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito.when(properties.idColumnName).thenReturn("header__id")
    val userFunctions = new CDCUserFunctions

    Given("A dataframe")
    val data = TestContexts.changeDummyData(10)
    When("The dataframe is sorted")
    val sorted = userFunctions.sortByKey(data, properties).collect()
    Then("THe order should be preserved")
    for (i <- sorted.indices) {
      sorted(i).getInt(0) should be(i + 1)
    }

    Given("A dataframe")
    val data2 =
      TestContexts.changeDummyData(10).sort(desc(properties.idColumnName))
    When("The dataframe is unsorted")
    val sorted2 = userFunctions.sortByKey(data, properties).collect()
    Then("The dataframe should be sorted")
    for (i <- sorted2.indices) {
      sorted2(i).getInt(0) should be(i + 1)
    }
  }

  "CDCUserFunctions" should "filter beforeimage rows" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito.when(properties.idColumnName).thenReturn("header__id")
    Mockito
      .when(properties.operationColumnName)
      .thenReturn("header__operation")
    Mockito.when(properties.operationColumnValueBefore).thenReturn("B")
    val userFunctions = new CDCUserFunctions

    Given("A dataframe")
    When("The dataframe contains beforeimage records")
    val data = TestContexts.changeDummyData(10)
    Then("The beforeimage records should be filtered")
    val result = userFunctions.filterBeforeRecords(data, properties).collect()
    result.length should be(8)
  }

  "CDCUserFunctions" should "set dates correctly" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    val userFunctions = new CDCUserFunctions

    Mockito.when(properties.operationColumnValueInsert).thenReturn("I")
    Mockito.when(properties.operationColumnValueUpdate).thenReturn("U")
    Mockito.when(properties.operationColumnValueDelete).thenReturn("D")

    Given("An insert operation")
    When("The record has been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord("I",
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2016-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be open")
    userFunctions.updateClosedRecord("I",
      null,
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "9999-12-31 23:59:59.000")

    Given("An Update operation")
    When("The record has been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord("U",
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2016-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be open")
    userFunctions.updateClosedRecord("U",
      null,
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "9999-12-31 23:59:59.000")

    Given("An delete operation")
    When("The record has been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord("D",
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2015-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord("D",
      null,
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2015-07-12 12:12:12.0000")
  }

  "CDCUserFunctions" should "close records" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    val userFunctions = new CDCUserFunctions

    Mockito.when(properties.operationColumnValueInsert).thenReturn("I")
    Mockito.when(properties.operationColumnValueUpdate).thenReturn("U")
    Mockito.when(properties.operationColumnValueDelete).thenReturn("D")
    Mockito.when(properties.operationColumnValueBefore).thenReturn("B")
    Mockito.when(properties.idColumnName).thenReturn("id")
    Mockito.when(properties.transactionTimeStampColumnName).thenReturn("header__timeStamp")
    Mockito.when(properties.validToColumnName).thenReturn("validto")
    Mockito.when(properties.validFromColumnName).thenReturn("validfrom")
    Mockito.when(properties.operationColumnName).thenReturn("header__operation")

    val noBeforeRows = userFunctions.filterBeforeRecords(TestContexts.changeDummyData(10).unionAll(TestContexts.changeDummyData(10)), properties)
    val data = userFunctions.closeRecords(noBeforeRows, properties).collect()

    for (i <- 0 until data.length - 1) {
      //updates
      if (data(i).getString(2).contains("U") && data(i).getInt(0) == data(i + 1).getInt(0)) {
        data(i).getString(8) should be (data(i + 1).getString(7))
      } //inserts
      else if (data(i).getString(2).contains("I") && data(i).getInt(0) == data(i + 1).getInt(0)) {
        data(i).getString(8) should be (data(i + 1).getString(7))
      } //deletes
      else if (data(i).getString(2).contains("D")) {
        data(i).getString(8) should be (data(i).getString(3))
      }
    }
  }
}

package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.{ GeneratedData, TestContexts }
import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.SparkException
import org.apache.spark.sql.{ AnalysisException, Row }
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.mockito.mock.SerializableMode
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import org.apache.spark.sql.functions._
import org.joda

/**
 * Unit tests for CDCUserFunctions
 */
class CDCUserFunctionsSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCUserFunctions" should "Return the current time in hive format" in {
    Given("A date")
    val userFunctions = new CDCUserFunctions
    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed(date.getMillis)
    Then("The date should me formatted to match")
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
    When("no rows are present in the DataFrame")
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
      userFunctions.groupByTransactionAndKey(TestContexts.dummyData(10),
        properties)
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
      .dropAttunityColumns(data, properties)
      .drop("validfrom")
      .drop("validto")
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

  "CDCUserFunctions" should "sort a DataFrame by business key" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito.when(properties.idColumnName).thenReturn("header__id")
    val userFunctions = new CDCUserFunctions

    Given("A DataFrame")
    val data = TestContexts.changeDummyData(10)
    When("The DataFrame is sorted")
    val sorted = userFunctions.sortByKey(data, properties).collect()
    Then("THe order should be preserved")
    for (i <- sorted.indices) {
      sorted(i).getInt(0) should be(i + 1)
    }

    Given("A DataFrame")
    val data2 =
      TestContexts.changeDummyData(10).sort(desc(properties.idColumnName))
    When("The DataFrame is unsorted")
    val sorted2 = userFunctions.sortByKey(data2, properties).collect()
    Then("The DataFrame should be sorted")
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

    Given("A DataFrame")
    When("The DataFrame contains beforeimage records")
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
    userFunctions.updateClosedRecord(isDeleted = false,
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2016-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be open")
    userFunctions.updateClosedRecord(isDeleted = false,
      null,
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "9999-12-31 23:59:59.000")

    Given("An Update operation")
    When("The record has been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord(isDeleted = false,
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2016-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be open")
    userFunctions.updateClosedRecord(isDeleted = false,
      null,
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "9999-12-31 23:59:59.000")

    Given("An delete operation")
    When("The record has been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord(isDeleted = true,
      "2016-07-12 12:12:12.0000",
      "2015-07-12 12:12:12.0000",
      properties) should be(
        "2015-07-12 12:12:12.0000")
    When("The record has not been superseded")
    Then("The record should be closed")
    userFunctions.updateClosedRecord(isDeleted = true,
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
    Mockito
      .when(properties.transactionTimeStampColumnName)
      .thenReturn("header__timeStamp")
    Mockito.when(properties.validToColumnName).thenReturn("validto")
    Mockito.when(properties.validFromColumnName).thenReturn("validfrom")
    Mockito
      .when(properties.operationColumnName)
      .thenReturn("header__operation")

    val noBeforeRows = userFunctions.addDeleteFlagColumn(
      userFunctions.filterBeforeRecords(
        TestContexts
          .changeDummyData(10)
          .unionAll(TestContexts.changeDummyData(10)),
        properties),
      "isDeleted",
      properties)
    val data = userFunctions
      .closeRecords(noBeforeRows, properties, "isDeleted", "header__timeStamp")
      .collect()

    for (i <- 0 until data.length - 1) {
      //updates
      if (data(i).getString(2).contains("U") && data(i).getInt(0) == data(
        i + 1).getInt(0)) {
        data(i).getString(9) should be(data(i + 1).getString(7))
      } //inserts
      else if (data(i).getString(2).contains("I") && data(i).getInt(0) == data(
        i + 1).getInt(0)) {
        data(i).getString(9) should be(data(i + 1).getString(7))
      } //deletes
      else if (data(i).getString(2).contains("D")) {
        data(i).getString(9) should be(data(i).getString(3))
      }
    }
  }

  "CDCUserFunctions" should "Add an active column to a DataFrame" in {
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
    Mockito
      .when(properties.transactionTimeStampColumnName)
      .thenReturn("header__timeStamp")
    Mockito.when(properties.validToColumnName).thenReturn("validto")
    Mockito.when(properties.validFromColumnName).thenReturn("validfrom")
    Mockito
      .when(properties.operationColumnName)
      .thenReturn("header__operation")
    Mockito.when(properties.activeColumnName).thenReturn("active")

    val noBeforeRows = userFunctions.filterBeforeRecords(
      TestContexts
        .changeDummyData(10)
        .unionAll(TestContexts.changeDummyData(10)),
      properties)
    val data = userFunctions.addActiveColumn(
      userFunctions.closeRecords(
        userFunctions.addDeleteFlagColumn(noBeforeRows,
          "isDeleted",
          properties),
        properties,
        "isDeleted",
        "header__timeStamp"),
      properties)
    data.collect().foreach { row =>
      if (row
        .getAs[String]("validto")
        .equalsIgnoreCase(userFunctions.activeDate)) {
        row.getAs[Boolean]("active") should be(true)
      } else {
        row.getAs[Boolean]("active") should be(false)
      }
    }
  }

  "CDCUserFunctions" should "drop the active column from a DataFrame" in {
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
    Mockito
      .when(properties.transactionTimeStampColumnName)
      .thenReturn("header__timeStamp")
    Mockito.when(properties.validToColumnName).thenReturn("validto")
    Mockito.when(properties.validFromColumnName).thenReturn("validfrom")
    Mockito
      .when(properties.operationColumnName)
      .thenReturn("header__operation")
    Mockito.when(properties.activeColumnName).thenReturn("active")

    Given("a DataFrame")
    val noBeforeRows = userFunctions.filterBeforeRecords(
      TestContexts
        .changeDummyData(10)
        .unionAll(TestContexts.changeDummyData(10)),
      properties)
    When("The active column exists")

    val data = userFunctions.addActiveColumn(
      userFunctions.closeRecords(
        userFunctions.addDeleteFlagColumn(noBeforeRows,
          "isDeleted",
          properties),
        properties,
        "isDeleted",
        "header__timeStamp"),
      properties)
    Then("The column should be dropped")
    an[AnalysisException] should be thrownBy {
      userFunctions.dropColumn(data, properties.activeColumnName).select("active")
    }

    Given("a DataFrame")
    When("The active column does not exist")
    Then("No error should occur")
    userFunctions.dropColumn(TestContexts.dummyData(10), properties.activeColumnName)
  }

  "CDCUserFunctions" should "filter data on a time Window" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    val userFunctions = new CDCUserFunctions

    val time = new DateTime()

    Mockito.when(properties.attunityCutoff).
      thenReturn(DateTimeFormat.forPattern("YYYY-MM-DD HH:mm:ss.SSS").print(time.minusHours(1)))

    Mockito.when(properties.attunityDateFormat)
      .thenReturn("YYYY-MM-DD HH:mm:ss.SSS")
    Mockito
      .when(properties.transactionTimeStampColumnName)
      .thenReturn("header__timeStamp")

    val afterTime = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.minusHours(3)))
    val beforeTime = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time))
    val beforeTime1 = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.plusHours(1)))
    val beforeTime2 = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.plusHours(2)))
    val beforeTime3 = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.plusHours(3)))
    val beforeTime4 = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.plusHours(4)))
    val beforeTime5 = udf(
      () =>
        DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .print(time.plusHours(5)))
    val junkTime = udf(() => "VertWuzEre")

    Given("A DataFrame")
    val data = TestContexts
      .changeDummyData(20)
      .drop("header__timeStamp")
      .withColumn("header__timeStamp", afterTime())
      .unionAll(
        TestContexts
          .changeDummyData(2)
          .drop("header__timeStamp")
          .withColumn("header__timeStamp", beforeTime()))
      .unionAll(
        TestContexts
          .changeDummyData(2)
          .drop("header__timeStamp")
          .withColumn("header__timeStamp", beforeTime1()))
      .unionAll(
        TestContexts
          .changeDummyData(2)
          .drop("header__timeStamp")
          .withColumn("header__timeStamp", beforeTime2()))
      .unionAll(TestContexts
        .changeDummyData(2)
        .drop("header__timeStamp")
        .withColumn("header__timeStamp", beforeTime3()))
      .unionAll(TestContexts
        .changeDummyData(2)
        .drop("header__timeStamp")
        .withColumn("header__timeStamp", beforeTime4()))
      .unionAll(TestContexts
        .changeDummyData(2)
        .drop("header__timeStamp")
        .withColumn("header__timeStamp", beforeTime5()))
    println(time)
    println(time.minus(3))
    When("returnMature is true")
    Then("New records should be filtered")
    userFunctions.filterOnTimeWindow(data, properties).count() should be(20)
    When("returnMature is false")
    Then("Old records should be filtered")
    userFunctions
      .filterOnTimeWindow(data, properties, returnMature = false).count() should be(12)

    Given("A DataFrame")
    When("The date cannot be parsed")
    Then("An error should be raised")
    an[SparkException] should be thrownBy {
      userFunctions
        .filterOnTimeWindow(TestContexts
          .changeDummyData(2)
          .drop("header__timeStamp")
          .withColumn("header__timeStamp", junkTime()),
          properties)
        .count()
    }
  }

  "CDCUserFunctions" should "Filter a DataFrame on active records" in {
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))
    val userFunctions = new CDCUserFunctions

    val validTo = udf(() => "2017-01-10 12:34:22.111")
    Mockito.when(properties.validToColumnName).thenReturn("validto")
    Mockito.when(properties.activeColumnName).thenReturn("active")

    val activeRecords = TestContexts.changeDummyData(12)
    val inactiveRecords = TestContexts
      .changeDummyData(10)
      .drop(properties.validToColumnName)
      .withColumn(properties.validToColumnName, validTo())

    Given("A DataFrame with an active column")
    val data = userFunctions
      .addActiveColumn(inactiveRecords.unionAll(activeRecords), properties)

    When("Filtering inactive records")
    val active = userFunctions.filterOnActive(data, properties)
    Then("Only active records should be returned")
    active.count() should be(12)
    When("Filtering active records")
    val inactive =
      userFunctions.filterOnActive(data, properties, returnActive = false)
    Then("Only inactive records should be returned")
    inactive.count() should be(10)

    Given("A DataFrame without an active column")
    val data2 = TestContexts.changeDummyData(10)
    When("Filtering records")
    Then("An error should be raised")
    an[AnalysisException] should be thrownBy {
      userFunctions.filterOnActive(data2, properties)
    }
  }

  "CDCUserFunctions" should "Join DataFrames" in {
    val userFunctions = new CDCUserFunctions

    Given("Two DataFrames")
    val df1 = TestContexts.changeDummyData(10)
    val df2 = TestContexts.changeDummyData(10)
    When("Both dataframes contain data")
    val res1 = userFunctions.unionAll(df1, df2)
    Then("The DataFrames should be unioned")
    res1.count() should be(20)

    When("One of the dataframes is empty")
    val res2 = userFunctions.unionAll(df1, TestContexts.changeDummyData(0))
    Then("The DataFrames should be unioned")
    res2.count() should be(10)
  }

  "CDCUserFunctions" should "Add a delete flag to a DataFrame" in {
    val userFunctions = new CDCUserFunctions
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Mockito
      .when(properties.operationColumnName)
      .thenReturn("header__operation")
    Mockito.when(properties.operationColumnValueDelete).thenReturn("D")

    Given("A DataFrame")
    When("The override has been set")
    val data = userFunctions
      .addDeleteFlagColumn(TestContexts.changeDummyData(10),
        "isDeleted",
        properties,
        doNotSetFlag = true)
      .collect()
    Then("the delete flag should be unset for all records")
    data.foreach(row => row.getBoolean(9) should be(false))

    Given("A DataFrame")
    When("The override has not been set")
    val data2 = userFunctions
      .addDeleteFlagColumn(TestContexts.changeDummyData(10),
        "isDeleted",
        properties)
      .collect()
    Then(
      "the delete flag should be set for any records that have been deleted")
    data2.foreach { row =>
      if (row.getString(2).equalsIgnoreCase("D")) {
        row.getBoolean(9) should be(true)
      } else {
        row.getBoolean(9) should be(false)
      }
    }
  }

  "CDCUserFunctions" should "Drop a delete flag to from a DataFrame" in {
    val userFunctions = new CDCUserFunctions
    val properties =
      Mockito.mock(classOf[CDCProperties],
        Mockito
          .withSettings()
          .serializable(SerializableMode.ACROSS_CLASSLOADERS))

    Given("A DataFrame")
    When("The delete flag column exists")
    val data =
      userFunctions.addDeleteFlagColumn(TestContexts.changeDummyData(10),
        "isDeleted",
        properties,
        doNotSetFlag = true)
    Then("The column should be dropped")
    an[AnalysisException] should be thrownBy {
      userFunctions.dropColumn(data, "isDeleted").select("isDeleted")
    }

    Given("A DataFrame")
    val data2 = TestContexts.changeDummyData(10)
    When("The delete flag column does not exist")
    an[AnalysisException] should be thrownBy {
      userFunctions
        .dropColumn(data2, "isDeleted")
        .select("isDeleted")
    }
    Then("No error should occur")
    userFunctions.dropColumn(data2, "isDeleted")
  }
}

package enstar.globaldatahub.cdcloader.control

import enstar.globaldatahub.TestContexts
import enstar.globaldatahub.common.io.{
  DataFrameReader,
  DataFrameTableOperations,
  SQLReader
}
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils }
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for the control processor
 */
class ControlProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "Controlprocessor" should "Register the control table" in {
    val reader: DataFrameReader = Mockito.mock(classOf[DataFrameReader])
    val properties: GDHProperties = Mockito.mock(classOf[GDHProperties])
    val tableOperations: DataFrameTableOperations =
      Mockito.mock(classOf[DataFrameTableOperations])
    val controlProcessor: CDCControlProcessor = new CDCControlProcessor

    Given("The input /control/dir/")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.data.controlpath"))
      .thenReturn("/control/dir/")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.tables.control.name"))
      .thenReturn("control")
    And("The input is valid")
    Mockito
      .when(
        reader.read(TestContexts.sqlContext,
          properties.getStringProperty(
            "spark.cdcloader.path.data.controlpath"),
          None))
      .thenReturn(TestContexts.generateControlTable(10))
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any(classOf[DataFrame]),
          org.mockito.Matchers.anyString()))
      .thenCallRealMethod()
    Then("The control table should be created")

    controlProcessor.registerControlTable(TestContexts.sqlContext,
      reader,
      properties,
      tableOperations)
    TestContexts.sqlContext
      .sql("SELECT * FROM " + properties.getStringProperty(
        "spark.cdcloader.tables.control.name"))
      .count should be(10)

    Mockito
      .verify(properties, Mockito.times(4))
      .getStringProperty(org.mockito.Matchers.anyString())

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .registerTempTable(org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString())

    Given("The control table has been created")
    When("The control table is de-registered")
    Mockito
      .when(
        tableOperations.deRegisterTempTable(
          TestContexts.sqlContext,
          properties.getStringProperty("spark.cdcloader.tables.control.name")))
      .thenCallRealMethod()

    controlProcessor.deregisterControlTable(TestContexts.sqlContext,
      properties,
      tableOperations)

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .deRegisterTempTable(org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString())

    Then("an exception should be thrown")
    an[RuntimeException] should be thrownBy {
      TestContexts.sqlContext.sql(
        "SELECT * FROM " + properties.getStringProperty(
          "spark.cdcloader.tables.control.name"))
    }
  }

  "Controlprocessor" should "Retrieve the last sequence processed" in {
    val reader: DataFrameReader = Mockito.mock(classOf[DataFrameReader])
    val sqlReader: SQLReader = Mockito.mock(classOf[SQLReader])
    val properties: GDHProperties = Mockito.mock(classOf[GDHProperties])
    val tableOperations: DataFrameTableOperations =
      Mockito.mock(classOf[DataFrameTableOperations])
    val controlProcessor: CDCControlProcessor = new CDCControlProcessor

    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed(date.getMillis)

    Given("A populated control table")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.data.controlpath"))
      .thenReturn("/control/dir/")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.tables.control.name"))
      .thenReturn("control")
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity"))
      .thenReturn("YYYYMMDDHHmmSShh")
    Mockito
      .when(
        reader.read(TestContexts.sqlContext,
          properties.getStringProperty(
            "spark.cdcloader.path.data.controlpath"),
          None))
      .thenReturn(TestContexts.generateControlTable(10))
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any(classOf[DataFrame]),
          org.mockito.Matchers.anyString()))
      .thenCallRealMethod()
    controlProcessor.registerControlTable(TestContexts.sqlContext,
      reader,
      properties,
      tableOperations)
    And("The control table has 10 rows")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.sql.controlpath"))
      .thenReturn("/some/path")
    Mockito
      .when(
        sqlReader.getSQLString(TestContexts.sparkContext,
          properties.getStringProperty(
            "spark.cdcloader.path.sql.controlpath")))
      .thenReturn(
        "SELECT MAX(attunitychangeseq) FROM control WHERE attunitytablename = ")
    Then(
      "The ControlProcessor should return the seqence number of the 10th row")
    controlProcessor.getLastSequenceNumber(TestContexts.sqlContext,
      sqlReader,
      properties,
      "'policy'") should be(
        DateTimeFormat
          .forPattern(properties.getStringProperty(
            "spark.cdcloader.format.timestamp.attunity"))
          .print(date) +
          "0000000000000000010"
      )
    DateTimeUtils.setCurrentMillisSystem()

    Mockito
      .verify(properties, Mockito.times(6))
      .getStringProperty(org.mockito.Matchers.anyString())

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .registerTempTable(org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString())

    Mockito
      .verify(sqlReader, Mockito.times(1))
      .getSQLString(org.mockito.Matchers.any(classOf[SparkContext]),
        org.mockito.Matchers.anyString())
  }

  "Controlprocessor" should "generate a sequence where none exists" in {
    val reader: DataFrameReader = Mockito.mock(classOf[DataFrameReader])
    val sqlReader: SQLReader = Mockito.mock(classOf[SQLReader])
    val properties: GDHProperties = Mockito.mock(classOf[GDHProperties])
    val tableOperations: DataFrameTableOperations =
      Mockito.mock(classOf[DataFrameTableOperations])
    val controlProcessor: CDCControlProcessor = new CDCControlProcessor

    val date = new DateTime()
    DateTimeUtils.setCurrentMillisFixed(date.getMillis)

    Given("A unpopulated control table")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.data.controlpath"))
      .thenReturn("/control/dir/")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.tables.control.name"))
      .thenReturn("control")
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.format.timestamp.attunity"))
      .thenReturn("YYYYMMDDHHmmSShh")
    Mockito
      .when(
        reader.read(TestContexts.sqlContext,
          properties.getStringProperty(
            "spark.cdcloader.path.data.controlpath"),
          None))
      .thenReturn(TestContexts.generateControlTable(0))
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any(classOf[DataFrame]),
          org.mockito.Matchers.anyString()))
      .thenCallRealMethod()
    controlProcessor.registerControlTable(TestContexts.sqlContext,
      reader,
      properties,
      tableOperations)
    When("The control table has 10 rows")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.sql.controlpath"))
      .thenReturn("/some/path")
    Mockito
      .when(
        sqlReader.getSQLString(TestContexts.sparkContext,
          properties.getStringProperty(
            "spark.cdcloader.path.sql.controlpath")))
      .thenReturn(
        "SELECT MAX(attunitychangeseq) FROM control WHERE attunitytablename = ")
    Then(
      "The ControlProcessor should return the seqence number of the 10th row")
    controlProcessor.getLastSequenceNumber(TestContexts.sqlContext,
      sqlReader,
      properties,
      "'policy'") should be("0")
    DateTimeUtils.setCurrentMillisSystem()

    Mockito
      .verify(properties, Mockito.times(5))
      .getStringProperty(org.mockito.Matchers.anyString())

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .registerTempTable(org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString())

    Mockito
      .verify(sqlReader, Mockito.times(1))
      .getSQLString(org.mockito.Matchers.any(classOf[SparkContext]),
        org.mockito.Matchers.anyString())
  }

  "ControlProcessor" should "Identify whether a table is being loaded for the first time" in {
    val reader: DataFrameReader = Mockito.mock(classOf[DataFrameReader])
    val properties: GDHProperties = Mockito.mock(classOf[GDHProperties])
    val tableOperations: DataFrameTableOperations =
      Mockito.mock(classOf[DataFrameTableOperations])
    val controlProcessor: CDCControlProcessor = new CDCControlProcessor

    Given("A control table")
    Mockito
      .when(
        properties.getArrayProperty(
          "spark.cdcloader.columns.control.names.controlcolumnnames"))
      .thenReturn(
        Array[String]("attunitychangeseq",
          "starttime",
          "endtime",
          "attunityrecordcount",
          "attunitytablename",
          "directoryname"))
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.path.data.controlpath"))
      .thenReturn("/control/dir/")
    Mockito
      .when(
        properties.getStringProperty("spark.cdcloader.tables.control.name"))
      .thenReturn("control")
    Mockito
      .when(
        properties.getStringProperty(
          "spark.cdcloader.columns.control.name.tablename"))
      .thenReturn("attunitytablename")
    Mockito
      .when(
        reader.read(TestContexts.sqlContext,
          properties.getStringProperty(
            "spark.cdcloader.path.data.controlpath"),
          None))
      .thenReturn(TestContexts.generateControlTable(10))
    Mockito
      .when(
        tableOperations.registerTempTable(
          org.mockito.Matchers.any(classOf[DataFrame]),
          org.mockito.Matchers.anyString()))
      .thenCallRealMethod()
    controlProcessor.registerControlTable(TestContexts.sqlContext,
      reader,
      properties,
      tableOperations)
    When("The control table has rows for a source table")
    Then("isInitialLoad should be false")
    controlProcessor.isInitialLoad(TestContexts.sqlContext,
      "'policy'",
      properties) should be(false)

    When("The control table has no rows for a source table")
    Then("isInitialLoad should be true")
    controlProcessor.isInitialLoad(TestContexts.sqlContext,
      "'transaction'",
      properties) should be(true)

    controlProcessor.deregisterControlTable(TestContexts.sqlContext,
      properties,
      tableOperations)

    Mockito
      .verify(properties, Mockito.times(8))
      .getStringProperty(org.mockito.Matchers.anyString())

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .registerTempTable(org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString())

    Mockito
      .verify(tableOperations, Mockito.times(1))
      .deRegisterTempTable(org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString())
  }
}

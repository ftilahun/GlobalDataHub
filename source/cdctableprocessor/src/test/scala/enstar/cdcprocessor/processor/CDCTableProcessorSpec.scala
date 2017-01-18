package enstar.cdcprocessor.processor

import enstar.cdcprocessor.TestContexts
import enstar.cdcprocessor.io.{
  AvroDataFrameReader,
  AvroDataFrameWriter,
  DataFrameWriter
}
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCTableProcessor
 */
class CDCTableProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCTableProcessor" should "Process a table" in {
    val userFunctions = Mockito.mock(classOf[UserFunctions])
    val properties = Mockito.mock(classOf[CDCProperties])
    val sqlContext = Mockito.mock(classOf[SQLContext])
    val reader = Mockito.mock(classOf[AvroDataFrameReader])
    val writer = Mockito.mock(classOf[AvroDataFrameWriter])

    Given("A table processor")
    val tableProcessor = new CDCTableProcessor

    When("process is called")

    Mockito.when(properties.metricsOutputDir).thenReturn(None)

    tableProcessor
      .process(sqlContext, properties, reader, writer, userFunctions)

    Then("Both inputs should be read")
    Mockito
      .verify(reader, Mockito.times(2))
      .read(org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[Some[StorageLevel]]))

    Then("Mature changes should be filtered")
    Mockito
      .verify(userFunctions, Mockito.times(2))
      .filterOnTimeWindow(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties]),
        org.mockito.Matchers.any(classOf[Boolean])
      )

    Then("Transactions should be grouped for net changes")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .groupByTransactionAndKey(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Records should be closed")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .closeRecords(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.anyString()
      )

    Then("Records should be flagged as active")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .addActiveColumn(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Before image rows should be filtered")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .filterBeforeRecords(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Attunity columns should be dropped")
    Mockito
      .verify(userFunctions, Mockito.times(2))
      .dropAttunityColumns(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("A Delete flag should be added to columns")
    Mockito
      .verify(userFunctions, Mockito.times(2))
      .addDeleteFlagColumn(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[CDCProperties]),
        org.mockito.Matchers.any(classOf[Boolean])
      )

    Then("New changes should be merged with active records from history")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .unionAll(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[DataFrame])
      )

    Then("Active records should be filtered")
    Mockito
      .verify(userFunctions, Mockito.times(2))
      .filterOnActive(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties]),
        org.mockito.Matchers.any(classOf[Boolean])
      )

    Then("Temp columns should be removed")
    Mockito
      .verify(userFunctions, Mockito.times(5))
      .dropColumn(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.anyString()
      )

    Then("Outputs should be persisted to disk")
    Mockito
      .verify(userFunctions, Mockito.times(3))
      .countAndSave(
        org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[DataFrameWriter]),
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[StorageLevel])
      )
  }

  "CDCTableProcessor" should "Process changes to a table" in {
    val userFunctions = Mockito.mock(classOf[UserFunctions])
    val properties = Mockito.mock(classOf[CDCProperties])

    Given("A table processor")
    val tableProcessor = new CDCTableProcessor
    When("processChangeData is called")
    tableProcessor.processChangeData(TestContexts.changeDummyData(10),
      properties,
      userFunctions)

    Then("Transactions should be grouped for net changes")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .groupByTransactionAndKey(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Before image rows should be filtered")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .filterBeforeRecords(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )
  }
}

package enstar.cdcprocessor.processor

import enstar.cdcprocessor.TestContexts
import enstar.cdcprocessor.io.{DataFrameReader, DataFrameWriter}
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

/**
 * Unit tests for CDCTableProcessor
 */
class CDCTableProcessorSpec extends FlatSpec with GivenWhenThen with Matchers {

  "CDCTableProcessor" should "Process a table" in {
    val sqlContext = Mockito.mock(classOf[SQLContext])
    val reader = Mockito.mock(classOf[DataFrameReader])
    val userFunctions = Mockito.mock(classOf[UserFunctions])
    val properties = Mockito.mock(classOf[CDCProperties])

    Given("A table processor")
    val tableProcessor = new CDCTableProcessor
    When("process is called")
    tableProcessor.processChangeData(TestContexts.changeDummyData(10),properties, userFunctions)

    Then("Transactions should be grouped for net changes")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .groupByTransactionAndKey(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Records should be closed")
    Mockito
      .verify(userFunctions, Mockito.times(1)).
      closeRecords(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Before image rows should be filtered")
    Mockito
      .verify(userFunctions, Mockito.times(1)).
      filterBeforeRecords(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )

    Then("Attunity columns should be dropped")
    Mockito
      .verify(userFunctions, Mockito.times(1))
      .dropAttunityColumns(
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[CDCProperties])
      )
  }

  "CDCTableProcessor" should "Save a table" in {
    val tableProcessor = new CDCTableProcessor
    val sqlContext = Mockito.mock(classOf[SQLContext])
    val writer = Mockito.mock(classOf[DataFrameWriter])
    val properties = Mockito.mock(classOf[CDCProperties])
    val data = Mockito.mock(classOf[DataFrame])

    Given("A table processor")
    When("Save is called")
    tableProcessor.save(sqlContext, writer, properties, data)
    Then("The dataframe should be persisted to disk")
    Mockito
      .verify(writer, Mockito.times(1))
      .write(
        org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.any(classOf[String]),
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[Some[StorageLevel]])
      )
  }
}

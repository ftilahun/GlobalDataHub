package enstar.cdctableprocessor.tests.unit.io

import enstar.cdctableprocessor.TestContexts
import enstar.cdctableprocessor.exceptions.DataFrameWriteException
import enstar.cdctableprocessor.io.{ AvroDataFrameWriter, CDCDataFrameWriter }
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * unit tests for the CDCDataFrameWriter
 */
class CDCDataFrameWriterSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  private implicit val sqlContext = TestContexts.sqlContext

  "CDCDataFrameWriter" should "Write output data" in {
    val dataFrameWriter = Mockito.mock(classOf[AvroDataFrameWriter])
    val cdcDataFrameWriter = new CDCDataFrameWriter(dataFrameWriter)

    Given("The input \"/some/path\"")

    val data = TestContexts.dummyData(10)
    When("The data set contains 10 rows")
    data.count should be(10)
    Then("10 rows should be persisted to disk")
    Mockito
      .when(
        dataFrameWriter
          .write("/some/path", data, Some(StorageLevel.MEMORY_ONLY)))
      .thenReturn(10)
    val outCount = cdcDataFrameWriter.write("/some/path/",
      data,
      Some(StorageLevel.MEMORY_ONLY))
    outCount should be(10)

    Given("The input \"/some/existing/path\"")
    When("The path already exists")
    Mockito
      .when(
        dataFrameWriter
          .write("/some/existing/path", data, Some(StorageLevel.MEMORY_ONLY)))
      .thenThrow(classOf[AnalysisException])
    Then("An exception should be raised")
    an[DataFrameWriteException] should be thrownBy {
      cdcDataFrameWriter.write("/some/existing/path",
        data,
        Some(StorageLevel.MEMORY_ONLY))
    }

    Mockito
      .verify(dataFrameWriter, Mockito.times(2))
      .write(org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[DataFrame]),
        org.mockito.Matchers.any(classOf[Option[StorageLevel]]))(
          org.mockito.Matchers.any(classOf[SQLContext]))
  }

}

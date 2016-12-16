package enstar.cdcprocessor.io

import enstar.cdcprocessor.TestContexts
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for the CDCDataFrameReader
 */
class CDCDataFrameReaderSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCDataFrameReader" should "Read input data" in {
    val dataFrameReader = Mockito.mock(classOf[AvroDataFrameReader])
    val cdcDataFrameReader = new CDCDataFrameReader(dataFrameReader)

    Mockito
      .when(dataFrameReader.read(TestContexts.sqlContext, "/some/path/", None))
      .thenReturn(TestContexts.dummyData(5))

    Given("The input \"/some/path/\"")
    val df =
      cdcDataFrameReader.read(TestContexts.sqlContext, "/some/path/", None)
    When("The dataset contains 5 rows")
    Then("Then a Dataframe should be returned with 5 rows")
    df.count should be(5)

    Given("The input \"/some/invalid/path/\"")
    Mockito
      .when(
        dataFrameReader
          .read(TestContexts.sqlContext, "/some/invalid/path/", None))
      .thenThrow(classOf[InvalidInputException])
    When("The path does not exist")
    Then("An exception should be raised")
    an[PathNotFoundException] should be thrownBy {
      cdcDataFrameReader.read(TestContexts.sqlContext,
        "/some/invalid/path/",
        None)
    }

    Mockito
      .verify(dataFrameReader, Mockito.times(2))
      .read(org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[Option[StorageLevel]]))
  }

}

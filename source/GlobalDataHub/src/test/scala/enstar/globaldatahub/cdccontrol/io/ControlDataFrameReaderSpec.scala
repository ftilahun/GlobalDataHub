package enstar.globaldatahub.cdccontrol.io

import enstar.globaldatahub.common.io.DataFrameReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for ControlDataFrameReader
 */
class ControlDataFrameReaderSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "ControlDataFrameReader" should "read a dataframe" in {

    Given("A ControlDataFrameReader")
    val dfReader = Mockito.mock(classOf[DataFrameReader])
    val sqlContext = Mockito.mock(classOf[SQLContext])
    val path = "/some/path"
    val storageLevelOption = Mockito.mock(classOf[Option[StorageLevel]])

    val controlDataFrameReader = new ControlDataFrameReader(dfReader)

    When("read is called")
    controlDataFrameReader.read(
      sqlContext,
      path,
      storageLevelOption
    )

    Then("The call should defer to the passed in reader")
    Mockito
      .verify(dfReader, Mockito.times(1))
      .read(
        org.mockito.Matchers.any(classOf[SQLContext]),
        org.mockito.Matchers.anyString(),
        org.mockito.Matchers.any(classOf[Option[StorageLevel]])
      )
  }
}

package com.kainos.enstar.globaldatahub.cdccontrol.io

import com.kainos.enstar.globaldatahub.common.io.DataFrameWriter
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for ControlDataFrameWriter
 */
class ControlDataFrameWriterSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "ControlDataFrameWriter" should "Write a DataFrame" in {

    Given( "A ControlDataFrameReader" )
    val dfWriter = Mockito.mock( classOf[DataFrameWriter] )
    val sqlContext = Mockito.mock( classOf[SQLContext] )
    val data = Mockito.mock( classOf[DataFrame] )
    val storageLevelOption = Mockito.mock( classOf[Option[StorageLevel]] )

    val path = "/some/path"
    val controlDataFrameWriter = new ControlDataFrameWriter( dfWriter )

    When( "write is called" )
    controlDataFrameWriter.write(
      sqlContext,
      path,
      data,
      storageLevelOption
    )

    Then( "The call should defer to the passed in writer" )
    Mockito.verify( dfWriter, Mockito.times( 1 ) ).write(
      org.mockito.Matchers.any( classOf[SQLContext] ),
      org.mockito.Matchers.anyString(),
      org.mockito.Matchers.any( classOf[DataFrame] ),
      org.mockito.Matchers.any( classOf[Option[StorageLevel]] )
    )
  }
}

package com.kainos.enstar.globaldatahub.io

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import org.apache.spark.sql.{ DataFrame, DataFrameWriter, SQLContext }
import org.mockito.Mockito
import com.databricks.spark.avro._
import com.kainos.enstar.globaldatahub.TestContexts$

/**
 * Created by ciaranke on 09/11/2016.
 */
class DataFrameIOSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  def takeStringReturnUnit( s : String ) = {}

  "DataFrameIO" should "write to HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave a comment to that effect?
      */
  }

  "DataFrameIO" should "read from HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave a comment to that effect?
      */
  }

}

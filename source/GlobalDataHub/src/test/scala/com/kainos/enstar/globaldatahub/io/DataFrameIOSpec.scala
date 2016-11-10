package com.kainos.enstar.globaldatahub.io

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCSQLReaderIO
 */
class DataFrameIOSpec extends FlatSpec with GivenWhenThen with Matchers {

  def takeStringReturnUnit( s : String ) = {}

  "DataFrameIO" should "write to HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave this comment to that effect?
   */
  }

  "DataFrameIO" should "read from HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave this comment to that effect?
   */
  }

}

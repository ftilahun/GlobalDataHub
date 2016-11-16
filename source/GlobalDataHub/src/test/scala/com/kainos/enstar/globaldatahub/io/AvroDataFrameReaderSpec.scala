package com.kainos.enstar.globaldatahub.io

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for AvroDataFrameReader
 */
class AvroDataFrameReaderSpec extends FlatSpec with GivenWhenThen with Matchers {

  "AvroDataFrameReader" should "read from HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave this comment to that effect?
   */
  }

}
package com.kainos.enstar.globaldatahub.io

import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for AvroDataFrameWriter
 */
class AvroDataFrameWriterSpec extends FlatSpec with GivenWhenThen with Matchers {

  "AvroDataFrameWriter" should "write to HDFS" in {
    /*I'm unable to mock this, should we:
      - delete then tests
      - use files
      - leave this comment to that effect?
   */
  }

}

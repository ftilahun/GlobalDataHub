package com.kainos.enstar.transformation

import com.kainos.enstar.transformation.sourcetype.SourceType
import org.scalatest.Tag

/**
 * Created by neilri on 11/01/2017.
 */
trait QuerySuite {
  val sourceType : SourceType
  def queryTestSets : List[QueryTestSet]
  def testTags : List[Tag] = List()
}

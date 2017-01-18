package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import org.scalatest.Tag

/**
 * Created by neilri on 17/01/2017.
 */
class LineOfBusinessQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Ndex

  override def testTags : List[Tag] = List( tags.LineOfBusiness )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "LineOfBusiness",
      "lineofbusiness",
      "LineOfBusiness.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "lineofbusiness", "PrimaryTestData.csv" ),
          order = List( "lineofbusinesscode" )
        )
      )
    )
  )

}

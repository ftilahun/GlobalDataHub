package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import org.scalatest.Tag

/**
 * Created by neilri on 17/01/2017.
 */
class MethodOfPlacementQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Ndex

  override def testTags : List[Tag] = List( tags.MethodOfPlacement )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "MethodOfPlacement",
      "methodofplacement",
      "MethodOfPlacement.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "methodofplacement", "PrimaryTestData.csv" )
        )
      )
    )
  )
}

package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.Ndex

/**
 * Created by neilri on 17/01/2017.
 */
class UnderwriterQuerySuite extends QuerySuite {
  override val sourceType = Ndex

  override def testTags = List( tags.Underwriter )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Underwriter",
      "underwriter",
      "Underwriter.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "underwriter", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "underwriter", "PrimaryTestData.csv" )
        )
      )
    )
  )
}

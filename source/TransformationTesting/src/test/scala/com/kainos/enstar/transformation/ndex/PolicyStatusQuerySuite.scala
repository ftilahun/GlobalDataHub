package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._

/**
 * Created by neilri on 17/01/2017.
 */
class PolicyStatusQuerySuite extends QuerySuite {
  override val sourceType = sourcetype.Ndex

  override def testTags = List( tags.PolicyStatus )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "PolicyStatus",
      "policystatus",
      "PolicyStatus.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_line_status", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policystatus", "PrimaryTestData.csv" )
        )
      )
    )
  )
}

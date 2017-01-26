package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.Ndex

/**
 * Created by neilri on 11/01/2017.
 */
class BranchQuerySuite extends QuerySuite {

  val sourceType = Ndex

  override def testTags = List( tags.Branch )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Branch",
      "branch",
      "Branch.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "branch", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" )
          ),
          "branch",
          "Branch/RecordCount.hql",
          "Branch/RecordCount.hql"
        )
      )
    )
  )
}

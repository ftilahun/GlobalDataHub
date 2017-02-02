package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.Ndex

/**
 * Created by neilri on 17/01/2017.
 */
class TrustFundQuerySuite extends QuerySuite {

  override val sourceType = Ndex

  override def testTags = List( tags.TrustFund )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "TrustFund",
      "trustfund",
      "TrustFund.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "trustfund", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_trust_fund", "PrimaryTestData.csv" )
          ),
          "trustfund",
          "TrustFund/RecordCount.hql",
          "TrustFund/RecordCount.hql"
        )
      )
    )
  )
}

package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._

/**
 * Created by neilri on 17/01/2017.
 */
class RiskCodeQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Ndex

  override def testTags = List( tags.RiskCode )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "RiskCode",
      "riskcode",
      "RiskCode.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_risk_code", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "riskcode", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_risk_code", "PrimaryTestData.csv" )
          ),
          "riskcode",
          "RiskCode/RecordCount.hql",
          "RiskCode/RecordCount.hql"
        )
      )
    )
  )
}

package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._

/**
 * Created by neilri on 17/01/2017.
 */
class PolicyEventTypeQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Ndex

  override def testTags = List( tags.PolicyEventType )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "PolicyEventType",
      "policyeventtype",
      "PolicyEventType.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_renewal_status", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policyeventtype", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_renewal_status", "PrimaryTestData.csv" )
          ),
          "policyeventtype",
          "PolicyEventType/RecordCount.hql",
          "PolicyEventType/RecordCount.hql"
        )
      )
    )
  )
}

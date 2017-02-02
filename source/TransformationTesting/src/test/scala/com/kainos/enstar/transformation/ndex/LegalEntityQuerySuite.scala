package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class LegalEntityQuerySuite extends QuerySuite {
  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.LegalEntity )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Legal Entity",
      "legalentity",
      "LegalEntity.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "legalentity", "PrimaryTestData.csv" ),
          order = List( "legalentitycode" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" )
          ),
          "legalentity",
          "LegalEntity/RecordCount.hql",
          "LegalEntity/RecordCount.hql"
        )
      )
    )
  )
}

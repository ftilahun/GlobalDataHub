package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class InsuredQuerySuite extends QuerySuite {
  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.Insured )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Insured",
      "insured",
      "Insured.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "organisation", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "insured", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "organisation", "PrimaryTestData.csv" )
          ),
          "insured",
          "Insured/RecordCount.hql",
          "Insured/RecordCount.hql"
        )
      )
    )
  )
}

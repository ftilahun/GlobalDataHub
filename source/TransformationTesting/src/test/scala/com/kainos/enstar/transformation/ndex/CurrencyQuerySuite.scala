package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.Ndex

/**
 * Created by neilri on 16/01/2017.
 */
class CurrencyQuerySuite extends QuerySuite {

  val sourceType = Ndex

  override def testTags = List( tags.Currency )

  override def queryTestSets : List[QueryTestSet] = List (
    QueryTestSet(
      "Currency",
      "currency",
      "Currency.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_currency", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "currency", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_currency", "PrimaryTestData.csv" )
          ),
          "currency",
          "Currency/RecordCount.hql",
          "Currency/RecordCount.hql"
        )
      )

    )
  )
}

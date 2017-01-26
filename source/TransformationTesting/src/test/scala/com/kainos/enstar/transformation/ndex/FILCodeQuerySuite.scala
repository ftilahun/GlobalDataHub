package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class FILCodeQuerySuite extends QuerySuite {

  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.FILCode )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "FILCode",
      "filcode",
      "FILCode.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_fil_code", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "filcode", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_fil_code", "PrimaryTestData.csv" )
          ),
          "filcode",
          "FILCode/RecordCount.hql",
          "FILCode/RecordCount.hql"
        )
      )
    )
  )
}

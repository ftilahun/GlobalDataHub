package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class DeductionTypeQuerySuite extends QuerySuite {

  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.DeductionType )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "DeductionType",
      "deductiontype",
      "DeductionType.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "deductiontype", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" )
          ),
          "deductiontype",
          "DeductionType/RecordCount.hql",
          "DeductionType/RecordCount.hql"
        )
      )
    )
  )
}

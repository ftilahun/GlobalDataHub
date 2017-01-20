package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }
import com.kainos.enstar.transformation.{ QueryTest, _ }

/**
 * Created by neilri on 16/01/2017.
 */
class DeductionQuerySuite extends QuerySuite {

  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.Deduction )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Deduction",
      "deduction",
      "Deduction.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "deduction", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "Multiple lines each with multiple deductions",
          Set(
            CsvSourceData( "line", "MultipleLines.csv" ),
            CsvSourceData( "layer", "MultipleLines.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionMonotonicSeqMultipleLines.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionMonotonicSeqMultipleLines.csv" )
        )
      )
    )
  )
}

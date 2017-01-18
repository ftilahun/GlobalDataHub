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
          "Calculating CalculatedDeductionAmount for policy with single ndex.deduction",
          Set(
            CsvSourceData( "line", "SingleDeductionCalculatedDeductionAmount.csv" ),
            CsvSourceData( "layer", "SingleDeductionCalculatedDeductionAmount.csv" ),
            CsvSourceData( "layer_deduction", "SingleDeductionCalculatedDeductionAmount.csv" )
          ),
          CsvSourceData( "deduction", "SingleDeductionCalculatedDeductionAmount.csv" )
        ),
        QueryTest(
          "Multiple deductions on a line, monotonic sequence no",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionMonotonicSeq.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionMonotonicSeq.csv" )
        ),
        QueryTest(
          "Multiple lines each with multiple deductions",
          Set(
            CsvSourceData( "line", "MultipleLines.csv" ),
            CsvSourceData( "layer", "MultipleLines.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionMonotonicSeqMultipleLines.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionMonotonicSeqMultipleLines.csv" )
        ),
        QueryTest(
          "Multiple deductions all with the same sequence",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionAllSameSeqNo.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionAllSameSeqNo.csv" )
        ),
        QueryTest(
          "Multiple deductions on a line not monotonic sequence",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionNonMonotonicSeq.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionNonMonotonicSeq.csv" )
        ),
        QueryTest(
          "Multiple ndex.deduction on a line not monotonic sequence and out of order",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionNonMonotonicSeqOutOfOrder.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionNonMonotonicSeqOutOfOrder.csv" ),
          order = List( "deductionsequence" )
        ),
        QueryTest(
          "Multiple deductions on a line not monotonic sequence with multiple of same sequence no",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MultipleDeductionNonMonotonicMultipleofSameSeq.csv" )
          ),
          CsvSourceData( "deduction", "MultipleDeductionNonMonotonicMultipleofSameSeq.csv" )
        )
      )
    )
  )
}

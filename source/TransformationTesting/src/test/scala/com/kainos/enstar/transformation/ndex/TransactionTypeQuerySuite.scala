package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation.{ QueryTestSet, _ }
import com.kainos.enstar.transformation.sourcetype.Ndex

/**
 * Created by neilri on 17/01/2017.
 */
class TransactionTypeQuerySuite extends QuerySuite {

  override val sourceType = Ndex

  override def testTags = List( tags.TransactionType )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "TransactionType - WrittenDeductions",
      "transactiontype/writtendeductions",
      "TransactionTypeWrittenDeduction.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "transactiontype", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" )
          ),
          "transactiontype",
          "TransactionType/WrittenDeductionsRecordCount.hql",
          "TransactionType/WrittenDeductionsRecordCount.hql"
        )
      )
    ),
    QueryTestSet(
      "TransactionType - WrittenPremium",
      "transactiontype/writtenpremium",
      "TransactionTypeWrittenPremium.hql",
      Set(
        QueryTest(
          "Primary",
          Set(),
          CsvSourceData( "transactiontype", "PrimaryTestData.csv" )
        )
      )
    )
  )
}

package com.kainos.enstar.transformation.genius

import com.kainos.enstar.transformation.sourcetype.Genius
import com.kainos.enstar.transformation.{ QueryTestSet, _ }

class TransactionTypeQuerySuite extends QuerySuite {

  override val sourceType = Genius

  override def testTags = List( tags.TransactionType )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "TransactionType - WrittenDeductionOurShare",
      "transactiontype/writtendeductionourshare",
      "TransactionTypeWrittenDeductionOurShare.hql",
      Set(
        QueryTest(
          "Primary",
          Set(
            CsvSourceData( "ICDCREP", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "transactiontype", "PrimaryTestData.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "ICDCREP", "PrimaryTestData.csv" )
          ),
          "transactiontype",
          "TransactionType/WrittenDeductionsRecordCount.hql",
          "TransactionType/WrittenDeductionsRecordCount.hql"
        )
      )
    )
  )
}

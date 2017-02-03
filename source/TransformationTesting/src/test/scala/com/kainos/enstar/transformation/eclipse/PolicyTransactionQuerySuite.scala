package com.kainos.enstar.transformation.eclipse

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Eclipse, SourceType }

/**
 * Created by terences on 27/01/2017.
 */
class PolicyTransactionQuerySuite extends QuerySuite {

  override val sourceType : SourceType = Eclipse

  override def queryTestSets : List[QueryTestSet] = List(

    QueryTestSet(
      "PolicyTransaction - WrittenDeductions",
      "policytransaction/writtendeductions",
      "PolicyTransactionWrittenDeductions.hql",
      Set(
        QueryTest(
          "Primary Test Data",
          Set(
            CsvSourceData( "policydeduction", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "fintranscategory", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "Deduction Sequencing Test Data",
          Set(
            CsvSourceData( "policydeduction", "DeductionSequencing.csv" ),
            CsvSourceData( "policyline", "DeductionSequencing.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "DeductionSequencing.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "DeductionSequencing.csv" ),
            CsvSourceData( "fintranscategory", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "DeductionSequencing.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "policydeduction", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "fintranscategory", "PrimaryTestData.csv" )
          ),
          "policytransaction",
          "PolicyTransaction/WrittenDeductionsRecordCount.hql",
          "PolicyTransaction/WrittenDeductionsRecordCount.hql"
        )
      )
    )
  )
}

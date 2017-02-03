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
    ),
    QueryTestSet(
      "PolicyTransaction - WrittenPremium",
      "policytransaction/writtenpremium",
      "PolicyTransactionWrittenPremium.hql",
      Set(
        QueryTest(
          "mapping happy path data",
          Set(
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to objcode",
          Set(
            CsvSourceData( "objcode", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForObjcodeLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to policyendorsmnt",
          Set(
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForPolicyendorsmntLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing variations of the originalamount and settlementamount calculations",
          Set(
            CsvSourceData( "objcode", "CalculationVariationsTestData.csv" ),
            CsvSourceData( "policy", "CalculationVariationsTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "CalculationVariationsTestData.csv" ),
            CsvSourceData( "policyline", "CalculationVariationsTestData.csv" ),
            CsvSourceData( "policyprem", "CalculationVariationsTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationVariationsTestData.csv" ),
          order = List( "transactionreference" )
        )

      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" )
          ),
          "policytransaction",
          "PolicyTransaction/WrittenPremiumRecordCount.hql",
          "PolicyTransaction/WrittenPremiumRecordCount.hql"
        ),
        ReconciliationTest(
          "No match for left join",
          Set(
            CsvSourceData( "objcode", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyprem", "PrimaryTestData.csv" )
          ),
          "policytransaction",
          "PolicyTransaction/WrittenPremiumRecordCount.hql",
          "PolicyTransaction/WrittenPremiumRecordCount.hql"
        )
      )
    )
  )
}
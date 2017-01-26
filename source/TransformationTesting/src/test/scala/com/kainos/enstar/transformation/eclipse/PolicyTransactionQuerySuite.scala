package com.kainos.enstar.transformation.eclipse

import com.kainos.enstar.transformation._

class PolicyTransactionQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Eclipse

  override def testTags = List( tags.PolicyTransaction )

  override def queryTestSets : List[QueryTestSet] = List(

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
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" ),
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
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" ),
          order = List( "transactionreference" )
        )

      )
    )
  )

}

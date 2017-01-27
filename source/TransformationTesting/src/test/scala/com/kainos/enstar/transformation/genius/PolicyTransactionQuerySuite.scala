package com.kainos.enstar.transformation.genius

import com.kainos.enstar.transformation._

class PolicyTransactionQuerySuite extends QuerySuite {

  override val sourceType = sourcetype.Genius

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
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to zusfdf00",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to zuskdf00",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to zugsdf00",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to zugpdf00",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "NoMatchForLeftJoin.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForLeftJoin.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping testing no match for left join to zuspdf00",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "NoMatchForLeftJoin.csv" )
          ),
          CsvSourceData( "policytransaction", "NoMatchForLeftJoin.csv" ),
          order = List( "transactionreference" )
        )

      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" )
          ),
          "policytransaction",
          "PolicyTransaction/WrittenPremiumRecordCount.hql",
          "PolicyTransaction/WrittenPremiumRecordCount.hql"
        ),
        ReconciliationTest(
          "No match for left join",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "NoMatchForLeftJoin.csv" )
          ),
          "policytransaction",
          "PolicyTransaction/WrittenPremiumRecordCount.hql",
          "PolicyTransaction/WrittenPremiumRecordCount.hql"
        )
      )
    )
  )

}

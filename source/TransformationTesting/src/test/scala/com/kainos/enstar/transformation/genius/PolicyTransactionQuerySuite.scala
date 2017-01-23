package com.kainos.enstar.transformation.genius

import com.kainos.enstar.transformation.{ CsvSourceData, _ }
import com.kainos.enstar.transformation.sourcetype.{ Genius, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class PolicyTransactionQuerySuite extends QuerySuite {
  override val sourceType : SourceType = Genius

  override def testTags = List( tags.PolicyTransaction )

  override def queryTestSets : List[QueryTestSet] = List(

    QueryTestSet(
      "PolicyTransaction - WrittenDeductions",
      "policytransaction/writtendeductions",
      "PolicyTransactionWrittenDeductions.hql",
      Set(
        QueryTest(
          "mapping happy path data",
          Set(
            CsvSourceData( "zucedf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zucodf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zudddf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zudgdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zudvdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zueldf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugpdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zugsdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zumadf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zusfdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuskdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "zuspdf00", "PrimaryTestData.csv" ),
            CsvSourceData( "icdcrep", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction_writtendeductions", "PrimaryTestData.csv" )
        ) /*,
        QueryTest(
          "mapping monotonic sequence number multiple sequence groups",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "MonotonicSeqMultipleSeqGroups.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "MonotonicSeqMultipleSeqGroups.csv" )
        ),
        QueryTest(
          "mapping non-monotonic sequence number multiple sequence groups",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "NonMonotonicSeqMultipleSeqGroups.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NonMonotonicSeqMultipleSeqGroups.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping non-monotonic sequence number multiple sequence groups with contingent deductions",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "NonMonotonicSeqMultipleSeqGroupsWithContingent.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NonMonotonicSeqMultipleSeqGroups.csv" ),
          order = List( "transactionreference" )
        ),
        QueryTest(
          "mapping special characters in all input data",
          Set(
            CsvSourceData( "line", "RiskReference_SpecialCharacters.csv" ),
            CsvSourceData( "layer", "FILCode_SpecialCharacters.csv" ),
            CsvSourceData( "layer_deduction", "DeductionCode_SpecialCharacters.csv" ),
            CsvSourceData( "line_risk_code", "SpecialCharacters.csv" ),
            CsvSourceData( "lookup_deduction_type", "DeductionDescription_SpecialCharacters.csv" ),
            CsvSourceData( "layer_trust_fund", "Trustfundindicator_SpecialCharacters.csv" )
          ),
          CsvSourceData( "policytransaction", "SpecialCharacters.csv" )
        ),
        QueryTest(
          "mapping with null settlement_due_date and FIL_code input values",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "NullFilCodeAndInceptionDate.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "NullValues.csv" )
        ),
        QueryTest(
          "mapping testing no corresponding layer_trust_fund for a line",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "LayerIdNotInLine.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLayerTrustFund.csv" )
        ),
        QueryTest(
          "mapping testing no corresponding line_risk_code for a line",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "LineIdNotInLine.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLineRiskCode.csv" )
        ),
        QueryTest(
          "mapping testing no corresponding layer_trust_fund or line_risk_code for a line",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "LineIdNotInLine.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "LayerIdNotInLine.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLineRiskCodeAndLayerTrustFund.csv" )
        )*/
      )
    )
  )
}

package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }

/**
 * Created by neilri on 16/01/2017.
 */
class PolicyTransactionQuerySuite extends QuerySuite {
  override val sourceType : SourceType = Ndex

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
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_deduction", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_deduction_type", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" )
        ),
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
        )
      )
    ),
    QueryTestSet(
      "PolicyTransaction - WrittenPremium",
      "policytransaction/writtenpremium",
      "PolicyTransactionWrittenPremium.hql",
      Set(
        QueryTest(
          "happy path data",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "using null values in 2 input tables",
          Set(
            CsvSourceData( "line", "VariousNullValues.csv" ),
            CsvSourceData( "layer", "VariousNullValues.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "VariousNullValues.csv" )
        ),
        QueryTest(
          "with data testing OriginalAmount and SettlementAmount calculations",
          Set(
            CsvSourceData( "line", "BusinessTypeOutsideRange.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "AmountAndSettlementCalculation.csv" )
        ),
        QueryTest(
          "with data testing primary key generation when fields are null",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "LineIdNotInLine.csv" ),
            CsvSourceData( "layer_trust_fund", "LayerIdNotInLine.csv" )
          ),
          CsvSourceData( "policytransaction", "PrimaryKeyMissingFields.csv" )
        ),
        QueryTest(
          "testing the settlementamount and originalamount calculations when layer_trust_fund is null",
          Set(
            CsvSourceData( "line", "BusinessTypeOutsideRange.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "LayerIdNotInLine.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLayerTrustFund.csv" )
        ),
        QueryTest(
          "testing the settlementamount and originalamount calculations when line_risk_code is null",
          Set(
            CsvSourceData( "line", "BusinessTypeOutsideRange.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "LineIdNotInLine.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLineRiskCode.csv" )
        ),
        QueryTest(
          "testing the settlementamount and originalamount calculations when layer_trust_fund and line_risk_code are null",
          Set(
            CsvSourceData( "line", "BusinessTypeOutsideRange.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "LineIdNotInLine.csv" ),
            CsvSourceData( "layer_trust_fund", "LayerIdNotInLine.csv" )
          ),
          CsvSourceData( "policytransaction", "CalculationsWithNullLineRiskCodeAndLayerTrustFund.csv" )
        )
      )
    )
  )
}

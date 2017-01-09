package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.transformation.udf.NetAsPctOfGross
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/policytransaction_writtendeductions/input/"
  private val testDataOutputDirPath = "/policytransaction_writtendeductions/output/"
  private val policyTransactionWrittenDeductionsTransformationPath = "Transformation/PolicyTransactionWrittenDeductions.hql"

  test( "PolicyTransactionDeductions Transformation mapping happy path data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping monotonic sequence number multiple sequence groups" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_MonotonicSeqMultipleSeqGroups.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_MonotonicSeqMultipleSeqGroups.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping non-monotonic sequence number multiple sequence groups" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_NonMonotonicSeqMultipleSeqGroups.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_NonMonotonicSeqMultipleSeqGroups.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction.orderBy( "transactionreference" ), result.orderBy( "transactionreference" ) )

  }

  test( "PolicyTransactionDeductions Transformation mapping special characters in all input data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_RiskReference_SpecialCharacters.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_FILCode_SpecialCharacters.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_DeductionCode_SpecialCharacters.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_SpecialCharacters.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_DeductionDescription_SpecialCharacters.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_SpecialCharacters.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_Trustfundindicator_SpecialCharacters.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_SpecialCharacters.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping with null settlement_due_date and FIL_code input values" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_Null_FILCode.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_Null_SettlementDueDate.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_NullValues.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping testing no corresponding layer_trust_fund for a line" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_CalculationsWithNullLayerTrustFund.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping testing no corresponding line_risk_code for a line" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_LineIdNotInLine.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_CalculationsWithNullLineRiskCode.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping testing no corresponding layer_trust_fund or line_risk_code for a line" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_PrimaryTestData.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_LineIdNotInLine.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
    val settlement_schedule = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "settlement_schedule_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_CalculationsWithNullLayerTrustFundAndLineRiskCode.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

}

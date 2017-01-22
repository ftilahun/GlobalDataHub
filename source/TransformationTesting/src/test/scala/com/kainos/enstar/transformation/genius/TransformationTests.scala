package com.kainos.enstar.transformation.genius

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import com.kainos.enstar.transformation.udf.NetAsPctOfGross
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/genius/policytransaction_writtendeductions/input/"
  private val testDataOutputDirPath = "/genius/policytransaction_writtendeductions/expected/"
  private val policyTransactionWrittenDeductionsTransformationPath = "Transformation/genius/PolicyTransactionWrittenDeductions.hql"

  test( "PolicyTransactionDeductions Transformation mapping happy path data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val icdcrep = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "icdcrep/PrimaryTestData.csv" )
    val zucedf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zucedf00/PrimaryTestData.csv" )
    val zucodf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zucodf00/PrimaryTestData.csv" )
    val zudddf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zudddf00/PrimaryTestData.csv" )
    val zudgdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zudgdf00/PrimaryTestData.csv" )
    val zudvdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zudvdf00/PrimaryTestData.csv" )
    val zueldf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zueldf00/PrimaryTestData.csv" )
    val zugpdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zugpdf00/PrimaryTestData.csv" )
    val zugsdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zugsdf00/PrimaryTestData.csv" )
    val zumadf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zumadf00/PrimaryTestData.csv" )
    val zusfdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zusfdf00/PrimaryTestData.csv" )
    val zuskdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zuskdf00/PrimaryTestData.csv" )
    val zuspdf00 = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "zuspdf00/PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    icdcrep.registerTempTable( "icdcrep" )
    zucedf00.registerTempTable( "zucedf00" )
    zucodf00.registerTempTable( "zucodf00" )
    zudddf00.registerTempTable( "zudddf00" )
    zudgdf00.registerTempTable( "zudgdf00" )
    zudvdf00.registerTempTable( "zudvdf00" )
    zueldf00.registerTempTable( "zueldf00" )
    zugpdf00.registerTempTable( "zugpdf00" )
    zugsdf00.registerTempTable( "zugsdf00" )
    zumadf00.registerTempTable( "zumadf00" )
    zusfdf00.registerTempTable( "zusfdf00" )
    zuskdf00.registerTempTable( "zuskdf00" )
    zuspdf00.registerTempTable( "zuspdf00" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }
  /*
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
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping with null inception_date and FIL_code input values" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_NullFilCodeAndInceptionDate.csv" )
    val layer_deduction = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_deduction_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_deduction_type = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_deduction_type_PrimaryTestData.csv" )
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
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // Load expected result into dataframe
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policytransactionwrittendeductions_CalculationsWithNullLineRiskCodeAndLayerTrustFund.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyTransactionWrittenDeductionsTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }
*/
}

package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenPremium

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policytransaction_writtenpremium/input/"
  private val testDataOutputPath = "/policytransaction_writtenpremium/output/"
  private val policyTransactionWrittenPremiumTransformationPath = "Transformation/ndex/PolicyTransactionWrittenPremium.hql"

  test( "PolicyTransaction_WP_Transformation test with happy path data" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )

    // The expected output file includes a null option for transactionreference, riskcode and trustfundcode as these come through as nullable
    // in the transform due to the LEFT join on line_risk_code and layer_trust_fund tables.
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_PrimaryTestData.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )
  }

  test( "PolicyTransaction_WP_Transformation test using null values in 3 input tables" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_VariousNullValues.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_VariousNullValues.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )

    // The expected output file includes a null option for transactionreference, riskcode and trustfundcode as these come through as nullable
    // in the transform due to the LEFT join on line_risk_code and layer_trust_fund tables.
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_VariousNullValues.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )
  }

  test( "PolicyTransaction_WP_Transformation test with data testing OriginalAmount and SettlementAmount calculations" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // data file for line contains a business type outside of range to perform calculation
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_BusinessTypeOutsideRange.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )

    // The expected output file includes a null option for transactionreference, riskcode and trustfundcode as these come through as nullable
    // in the transform due to the LEFT join on line_risk_code and layer_trust_fund tables.
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_AmountAndSettlementCalculation.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransaction_WP_Transformation test with data testing primary key generation when fields are null" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_LineIdNotInLine.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // The expected output file includes a null option for transactionreference, riskcode and trustfundcode as these come through as nullable
    // in the transform due to the LEFT join on line_risk_code and layer_trust_fund tables.
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_PrimaryKeyMissingFields.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransaction_WP_Transformation testing the settlementamount and originalamount calculations when layer_trust_fund is null" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_BusinessTypeOutsideRange.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_PrimaryTestData.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // The values for settlementamount and originalamount have been calculated using the ELSE value of 100 for
    // layer_trust_fund.est_premium_split_pct as this value comes through as null in this case due to the LEFT JOIN to
    // the layer_trust_fund table
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_CalculationsWithNullLayerTrustFund.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransaction_WP_Transformation testing the settlementamount and originalamount calculations when line_risk_code is null" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_BusinessTypeOutsideRange.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_LineIdNotInLine.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )

    // The values for settlementamount and originalamount have been calculated using the ELSE value of 100 for
    // line_risk_code.risk_code_pct as this value comes through as null in this case due to the LEFT JOIN to
    // the line_risk_code table
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_CalculationsWithNullLineRiskCode.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransaction_WP_Transformation testing the settlementamount and originalamount calculations when layer_trust_fund and line_risk_code are null" ){

    // Arrange //
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_BusinessTypeOutsideRange.csv" )
    val layer = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_risk_code_LineIdNotInLine.csv" )
    val layer_trust_fund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_LayerIdNotInLine.csv" )

    // The values for settlementamount and originalamount have been calculated using the ELSE value of 100 for
    // layer_trust_fund.est_premium_split_pct and line_risk_code.risk_code_pct
    val expectedPolicyTransaction = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "policytransaction_CalculationsWithNullLineRiskCodeAndLayerTrustFund.csv" )

    val hqlStatement = utils.loadHQLStatementFromResource( policyTransactionWrittenPremiumTransformationPath )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    line_risk_code.registerTempTable( "line_risk_code" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

}

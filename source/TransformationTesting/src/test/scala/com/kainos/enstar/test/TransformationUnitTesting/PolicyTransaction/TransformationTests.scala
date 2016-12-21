package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policytransaction_writtenpremium/input/"
  private val testDataOutputPath = "/policytransaction_writtenpremium/output/"

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

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

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

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

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

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

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

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

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

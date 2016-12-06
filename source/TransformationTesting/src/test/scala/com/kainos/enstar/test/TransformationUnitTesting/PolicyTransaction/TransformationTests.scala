package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyTransactionUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/policytransaction_writtenpremium/input/"
  private val schemasPath = "/policytransaction_writtenpremium/schemas/"

  def populateDataFrameWithLayerTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTrustFundTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerTrustFundMapping,
      sqlc
    )
  }

  def populateDataFrameWithLineTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLinkRiskCodeTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupPremiumTypeTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "lookup_premium_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lookupPremiumTypeMapping,
      sqlc
    )
  }

  def populateDataFrameWithSettlementScheduleTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.settlementScheduleMapping,
      sqlc
    )
  }

  def populateDataFrameWithPolicyTransactionTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/output/" + dataFile ).toString,
      getClass.getResource( schemasPath + "policytransaction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.policyTransactionMapping,
      sqlc
    )
  }

  test( "PolicyTransaction_WP_Transformation test with happy path data" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    val sqlc = sqlContext

    // Load test data into dataframe
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val line_risk_code = populateDataFrameWithLinkRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )
    val lookup_premium_type = populateDataFrameWithLookupPremiumTypeTestData( "lookup_premium_type_PrimaryTestData.csv", sqlc )
    val settlement_schedule = populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_PrimaryTestData.csv", sqlc )

    val expectedPolicyTransaction = populateDataFrameWithPolicyTransactionTestData(
      "policytransaction_PrimaryTestData.csv",
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_premium_type.registerTempTable( "lookup_premium_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )
  }

  test( "PolicyTransaction_WP_Transformation test using null values in 3 input tables" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val line = populateDataFrameWithLineTestData( "line_VariousNullValues.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_VariousNullValues.csv", sqlc )
    val line_risk_code = populateDataFrameWithLinkRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )
    val lookup_premium_type = populateDataFrameWithLookupPremiumTypeTestData( "lookup_premium_type_PrimaryTestData.csv", sqlc )
    val settlement_schedule = populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_VariousNullValues.csv", sqlc )

    val expectedPolicyTransaction = populateDataFrameWithPolicyTransactionTestData(
      "policytransaction_VariousNullValues.csv",
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_premium_type.registerTempTable( "lookup_premium_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )
  }

  test( "PolicyTransaction_WP_Transformation test with data testing OriginalAmount and SettlementAmount calculations" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // data file for line contains a business type outside of range to perform calculation
    val line = populateDataFrameWithLineTestData( "line_BusinessTypeOutsideRange.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val line_risk_code = populateDataFrameWithLinkRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )
    val lookup_premium_type = populateDataFrameWithLookupPremiumTypeTestData( "lookup_premium_type_PrimaryTestData.csv", sqlc )
    val settlement_schedule = populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_PrimaryTestData.csv", sqlc )

    val expectedPolicyTransaction = populateDataFrameWithPolicyTransactionTestData(
      "policytransaction_AmountAndSettlementCalculation.csv",
      sqlc
    )

    // Load the hql statement under test
    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_premium_type.registerTempTable( "lookup_premium_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )

    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

}

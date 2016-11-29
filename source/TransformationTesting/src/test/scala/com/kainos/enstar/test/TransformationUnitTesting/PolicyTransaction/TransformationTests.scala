package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyTransactionUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 28/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "PolicyTransaction_WP_Transformation_test1 - standard data" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerMapping,
      sqlc
    )

    val layer_trust_fund : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/layer_trust_fund_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerTrustFundMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/line_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/line_risk_code_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineRiskCodeMapping,
      sqlc
    )

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/lookup_premium_type_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/lookup_premium_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lookupPremiumTypeMapping,
      sqlc
    )

    val settlement_schedule : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/settlement_schedule_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.settlementScheduleMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicyTransaction : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/output/policytransaction_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/policytransaction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.policyTransactionMapping,
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

  test( "PolicyTransaction_WP_Transformation_test2 - data with allowed nulls" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/layer_test2.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerMapping,
      sqlc
    )

    val layer_trust_fund : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/layer_trust_fund_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.layerTrustFundMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/line_test2.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/line_risk_code_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lineRiskCodeMapping,
      sqlc
    )

    val lookup_premium_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/lookup_premium_type_test1.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/lookup_premium_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.lookupPremiumTypeMapping,
      sqlc
    )

    val settlement_schedule : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/input/settlement_schedule_test2.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.settlementScheduleMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicyTransaction : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtenpremium/output/policytransaction_test2.csv" ).toString,
      getClass.getResource( "/policytransaction_writtenpremium/schemas/policytransaction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionUtils.policyTransactionMapping,
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

  // TODO
  test( "PolicyTransaction_WP_Transformation_test3 - data testing OriginalAmount and SettlementAmount calculations" ){

  }

}

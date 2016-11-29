package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyTransactionUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 28/11/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "PolicyTransaction - Reconciliation over test data" ) {

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

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransaction_WrittenPremium.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransaction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransaction/OutputRecordCount.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_premium_type.registerTempTable( "lookup_premium_type" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policytransaction" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}

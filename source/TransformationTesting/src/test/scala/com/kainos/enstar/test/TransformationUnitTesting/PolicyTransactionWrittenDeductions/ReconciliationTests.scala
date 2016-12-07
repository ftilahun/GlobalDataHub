package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{PolicyTransactionDeductionsUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by sionam on 07/12/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/layer.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerMapping,
      sqlc
    )

    val layer_deduction : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/layer_deduction.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer_deduction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerDeductionMapping,
      sqlc
    )

    val layer_trust_fund : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/layer_trust_fund.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerTrustFundMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/line.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineMapping,
      sqlc
    )

    val line_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/line_risk_code.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineRiskCodeMapping,
      sqlc
    )

    val lookup_deduction_type : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/lookup_deduction_type.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupDeductionTypeMapping,
      sqlc
    )

    val lookup_trust_fund : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/lookup_trust_fund.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupTrustFundMapping,
      sqlc
    )

    val lookup_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/lookup_risk_code.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupRiskCodeMapping,
      sqlc
    )

    val settlement_schedule : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/settlement_schedule.csv" ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.settlementScheduleMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransactionWrittenDeductions.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransactionWrittenDeductions/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyTransactionWrittenDeductions/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    lookup_trust_fund.registerTempTable( "lookup_trust_fund" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    settlement_schedule.registerTempTable( "settlement_schedule" )

    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policytransactionwrittendeductions" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}

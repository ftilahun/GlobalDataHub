package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{PolicyTransactionDeductionsUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

/**
 * Created by sionam on 05/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLineTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerDeductionTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer_deduction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerDeductionMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerTrustFundMapping,
      sqlc
    )
  }

  def populateDataFrameWithLineRiskCodeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupDeductionTypeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupDeductionTypeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupRiskCodeMappingTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithSettlementScheduleTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.settlementScheduleMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/lookup_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupTrustFundMapping,
      sqlc
    )
  }

  def populateDataFrameWithPolicyTransactionDeductionsTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransactionwrittendeductions/output/" + dataFileName ).toString,
      getClass.getResource( "/policytransactionwrittendeductions/schemas/policytransactionwrittendeductions.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.policyTransactionDeductionsMapping,
      sqlc
    )
  }

  test( "PolicyTransactionDeductions Transformation mapping happy path data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "risk_code.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "trust_fund_indicator.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund.csv", sqlc)

    // Load expected result into dataframe
    val expectedPolicyTransaction = this.populateDataFrameWithPolicyTransactionDeductionsTestData( "policytransactionwrittendeductions.csv", sqlc )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyTransactionWrittenDeductions.hql" )

    // Act //
    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layer_deduction.registerTempTable( "layer_deduction" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_deduction_type.registerTempTable( "lookup_deduction_type" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    settlement_schedule.registerTempTable( "settlement_schedule" )
    lookup_trust_fund.registerTempTable( "lookup_trust_fund" )
    layer_trust_fund.registerTempTable( "layer_trust_fund" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    println(expectedPolicyTransaction.count())
    println(result.count())
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

}

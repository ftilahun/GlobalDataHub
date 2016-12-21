package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{NetAsPctOfGross, PolicyTransactionDeductionsUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

/**
 * Created by sionam on 05/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLineTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/line.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/layer.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerDeductionTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/layer_deduction.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerDeductionMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/layer_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.layerTrustFundMapping,
      sqlc
    )
  }

  def populateDataFrameWithLineRiskCodeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/line_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lineRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupDeductionTypeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/lookup_deduction_type.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupDeductionTypeMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupRiskCodeMappingTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/lookup_risk_code.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupRiskCodeMapping,
      sqlc
    )
  }

  def populateDataFrameWithSettlementScheduleTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/settlement_schedule.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.settlementScheduleMapping,
      sqlc
    )
  }

  def populateDataFrameWithLookupTrustFundTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/input/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/lookup_trust_fund.avro" ).toString,
      _.split( "," ),
      PolicyTransactionDeductionsUtils.lookupTrustFundMapping,
      sqlc
    )
  }

  def populateDataFrameWithPolicyTransactionDeductionsTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( "/policytransaction_writtendeductions/output/" + dataFileName ).toString,
      getClass.getResource( "/policytransaction_writtendeductions/schemas/policytransactionwrittendeductions.avro" ).toString,
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
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction_PrimaryTestData.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_PrimaryTestData.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )

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
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping increasing sequence number" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction_IncreasingSequenceNo.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_PrimaryTestData.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicyTransaction = this.populateDataFrameWithPolicyTransactionDeductionsTestData( "policytransactionwrittendeductions_IncreasingSequenceNumber.csv", sqlc )

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
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping special characters in all input data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_RiskReference_SpecialCharacters.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_FILCode_SpecialCharacters.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction_DeductionCode_SpecialCharacters.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_SpecialCharacters.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type_DeductionDescription_SpecialCharacters.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "lookup_risk_code_SpecialCharacters.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_SpecialCharacters.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_SpecialCharacters.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_Trustfundindicator_SpecialCharacters.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicyTransaction = this.populateDataFrameWithPolicyTransactionDeductionsTestData( "policytransactionwrittendeductions_SpecialCharacters.csv", sqlc )

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
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping test with broken join which will not be in output" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction_PrimaryTestData.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_PrimaryTestData.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_BrokenJoin.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_BrokenJoin.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicyTransaction = this.populateDataFrameWithPolicyTransactionDeductionsTestData( "policytransactionwrittendeductions_BrokenJoin.csv", sqlc )

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
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

  test( "PolicyTransactionDeductions Transformation mapping with null settlement_due_date and FIL_code input values" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = this.populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = this.populateDataFrameWithLayerTestData( "layer_Null_FILCode.csv", sqlc )
    val layer_deduction = this.populateDataFrameWithLayerDeductionTestData( "layer_deduction_PrimaryTestData.csv", sqlc )
    val line_risk_code = this.populateDataFrameWithLineRiskCodeTestData( "line_risk_code_PrimaryTestData.csv", sqlc )
    val lookup_deduction_type = this.populateDataFrameWithLookupDeductionTypeTestData( "lookup_deduction_type_PrimaryTestData.csv", sqlc )
    val lookup_risk_code = this.populateDataFrameWithLookupRiskCodeMappingTestData( "lookup_risk_code_PrimaryTestData.csv", sqlc )
    val settlement_schedule = this.populateDataFrameWithSettlementScheduleTestData( "settlement_schedule_Null_SettlementDueDate.csv", sqlc )
    val lookup_trust_fund = this.populateDataFrameWithLookupTrustFundTestData( "lookup_trust_fund_PrimaryTestData.csv", sqlc )
    val layer_trust_fund = this.populateDataFrameWithLayerTrustFundTestData( "layer_trust_fund_PrimaryTestData.csv", sqlc )

    // Load expected result into dataframe
    val expectedPolicyTransaction = this.populateDataFrameWithPolicyTransactionDeductionsTestData( "policytransactionwrittendeductions_NullValues.csv", sqlc )

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
    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyTransaction, result )

  }

}

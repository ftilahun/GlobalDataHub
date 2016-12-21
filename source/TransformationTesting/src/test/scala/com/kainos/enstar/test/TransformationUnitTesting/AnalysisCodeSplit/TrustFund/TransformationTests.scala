package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplit.TrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/analysiscodesplit/trustfund/input/"
  private val testDataOutputPath = "/analysiscodesplit/trustfund/output/"
  private val analysisCodeSplitTrustFundTransformation = "Transformation/AnalysisCodeSplitTrustFund.hql"

  test( "AnalysisCodeSplitTrustFundTransformation mapping test one line row to one layer_trust_fund row" ){

    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_trust_fund_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "analysiscodesplit_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformation )

    // Act //
    line.registerTempTable( "line" )
    layerTrustFund.registerTempTable( "layer_trust_fund" )
    lookupTrustFund.registerTempTable( "lookup_trust_fund" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit.orderBy( "coveragereference" ), result.orderBy( "coveragereference" ) )
  }

  test( "AnalysisCodeSplitTrustFundTransformation mapping test many layer_trust_fund rows to one line row" ){

    implicit val sqlc = sqlContext

    // Arrange //
    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_ManyTrustFundToOneLayer.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_ManyTrustFundToOneLayer.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_trust_fund_ManyTrustFundToOneLayer.csv" )

    // Load expected result into dataframe
    val expectedAnalysisCodeSplit = utils.populateDataFrameFromCsvWithHeader( testDataOutputPath + "analysiscodesplit_ManyTrustFundToOneLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformation )

    // Act //
    line.registerTempTable( "line" )
    layerTrustFund.registerTempTable( "layer_trust_fund" )
    lookupTrustFund.registerTempTable( "lookup_trust_fund" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedAnalysisCodeSplit, result )
  }

}

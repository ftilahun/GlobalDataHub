package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplit.TrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/analysiscodesplit/trustfund/input/"
  private val analysisCodeSplitTrustFundTransformationPath = "Transformation/AnalysisCodeSplitTrustFund.hql"

  test( "Reconciliation on primary input test data for AnalysisCodeSplit TrustFund" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_PrimaryTestData.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_trust_fund_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformationPath )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitTrustFund/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitTrustFund/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    layerTrustFund.registerTempTable( "layer_trust_fund" )
    lookupTrustFund.registerTempTable( "lookup_trust_fund" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

  test( "Reconciliation on input test data with many trust funds to one layer for AnalysisCodeSplit TrustFund" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_ManyTrustFundToOneLayer.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "layer_trust_fund_ManyTrustFundToOneLayer.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_trust_fund_ManyTrustFundToOneLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformationPath )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitTrustFund/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitTrustFund/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    layerTrustFund.registerTempTable( "layer_trust_fund" )
    lookupTrustFund.registerTempTable( "lookup_trust_fund" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}

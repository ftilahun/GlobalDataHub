package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitTrustFund

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/analysiscodesplit/trustfund/input/"
  private val analysisCodeSplitTrustFundTransformation = "Transformation/AnalysisCodeSplitTrustFund.hql"

  test( "Reconciliation on test case 1 input test data for AnalysisCodeSplit TrustFund" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_PrimaryTestData.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_PrimaryTestData.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_trust_fund_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformation )
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

  test( "Reconciliation on test case 2 input test data for AnalysisCodeSplit TrustFund" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "line_ManyTrustFundToOneLayer.csv" )
    val layerTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "layer_trust_fund_ManyTrustFundToOneLayer.csv" )
    val lookupTrustFund = utils.populateDataFrameFromCsvWithHeader( testDataInputPath + "lookup_trust_fund_ManyTrustFundToOneLayer.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( analysisCodeSplitTrustFundTransformation )
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

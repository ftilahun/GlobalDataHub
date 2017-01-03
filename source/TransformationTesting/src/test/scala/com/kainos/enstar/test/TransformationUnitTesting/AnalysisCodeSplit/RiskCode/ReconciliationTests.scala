package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplit.RiskCode

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/analysiscodesplit/riskcode/input/"

  test( "Reconciliation on primary input test data for AnalysisCodeSplit RiskCode" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_PrimaryTestData.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_PrimaryTestData.csv" )
    val lookup_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_risk_code_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

  test( "Reconciliation on input test data for many line risks to one line for AnalysisCodeSplit RiskCode" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val line = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_ManyLineRiskToOneLine.csv" )
    val line_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "line_risk_code_ManyLineRiskToOneLine.csv" )
    val lookup_risk_code = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_risk_code_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/AnalysisCodeSplitRiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/AnalysisCodeSplitRiskCode/OutputRecordCount.hql" )

    // Act //
    line.registerTempTable( "line" )
    line_risk_code.registerTempTable( "line_risk_code" )
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "analysiscodesplit" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}